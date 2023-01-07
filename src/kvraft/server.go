package kvraft

import (
	"6.824/debug"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister      *raft.Persister
	stateMachine   KVStateMachine
	clientSessions map[int64]Session
	notifyChans    map[int]chan *CommandReply // 键值是对应日志的index，不是clientId
	lastApplied    int
}

func (kv *KVServer) ExecCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
		debug.Debug(debug.KVServer, "S%v Duplicate Command args%v", kv.me, args)
		lastReply := kv.clientSessions[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		debug.Debug(debug.KVServer, "S%v Reply Command args %v, rply {%v %v}", kv.me, args, reply.Err, len(reply.Value))
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	logIdx, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	debug.Debug(debug.KVServer, "S%v Command Has Started args%v", kv.me, args)
	ch := kv.getNotifyChan(logIdx)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	// delete outdated notifyChan
	delete(kv.notifyChans, logIdx)
	debug.Debug(debug.KVServer, "S%v Reply Command args %v, rply {%v %v}", kv.me, args, reply.Err, len(reply.Value))
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		for message := range kv.applyCh {
			if message.CommandValid {
				kv.mu.Lock()
				command := message.Command.(Command)

				if kv.lastApplied >= message.CommandIndex {
					debug.Debug(debug.KVServer, "S%v Recv Outdated Msg %v Due to newer Snapshot installed, Not Apply, lastApplied %v", kv.me, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply *CommandReply
				if !command.isReadOnly() && kv.isDuplicateRequest(command.ClientId, command.SequenceNum) {
					reply = kv.clientSessions[command.ClientId].LastReply
					debug.Debug(debug.KVServer, "S%v Apply Duplicate Command %v Result {%v, %v}",
						kv.me, command, reply.Err, len(reply.Value))
				} else {
					reply = kv.applyLog(&command)
					if !command.isReadOnly() {
						kv.clientSessions[command.ClientId] = Session{command.SequenceNum, reply}
					}
				}

				currentTerm, isLeader := kv.rf.GetState()
				if currentTerm == message.CommandTerm && isLeader {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- reply
				}

				if kv.needSnapshot() {
					debug.Debug(debug.KVSnap, "S%d KVServer Take Snapshot, Msg %v",
						kv.me, message)
					snapshot := kv.takeSnapshot()
					kv.rf.Snapshot(message.CommandIndex, snapshot)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				debug.Debug(debug.KVSnap, "S%d KVServer Try Install Snapshot %d",
					kv.me, message.SnapshotIndex)
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.installSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
					debug.Debug(debug.KVSnap, "S%d KVServer Install Snapshot %d Finished",
						kv.me, message.SnapshotIndex)
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyLog(command *Command) *CommandReply {
	var reply CommandReply
	switch command.Op {
	case OpGet:
		reply.Err, reply.Value = kv.stateMachine.Get(command.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(command.Key, command.Value)
	}
	return &reply
}

func (kv *KVServer) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.clientSessions)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine KVStateMachine
	var clientSessions map[int64]Session
	if d.Decode(&stateMachine) != nil || d.Decode(&clientSessions) != nil {
		debug.Debug(debug.DError, "S%d KVServer Cannot Deserialize State", kv.me)
	} else {
		kv.stateMachine, kv.clientSessions = stateMachine, clientSessions
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		maxraftstate:   maxraftstate,
		persister:      persister,
		stateMachine:   NewKVStateMachine(),
		clientSessions: make(map[int64]Session),
		notifyChans:    make(map[int]chan *CommandReply),
		lastApplied:    0,
	}
	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	go kv.applier()

	return kv
}

func (kv *KVServer) isDuplicateRequest(clientId int64, sequenceNum int) bool {
	// 不可能存在比lastCommandId还小；哪怕有，由于client已经发出commandId更大的command，因此client也已经不会接受该回复了
	session, ok := kv.clientSessions[clientId]
	return ok && sequenceNum <= session.LastSequenceNum
}

func (kv *KVServer) getNotifyChan(logIndex int) chan *CommandReply {
	if _, ok := kv.notifyChans[logIndex]; !ok {
		kv.notifyChans[logIndex] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[logIndex]
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate
}
