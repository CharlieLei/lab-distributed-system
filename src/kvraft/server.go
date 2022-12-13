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

const ClientRequestTimeout = 100 * time.Millisecond

type Command struct {
	ClientId  int64
	CommandId int
	Op        OpType
	Key       string
	Value     string
}

type Session struct {
	LastCommandId int
	LastReply     CommandReply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmap          map[string]string
	clientSessions map[int64]Session
	notifyChans    map[int]chan CommandReply // 键值是对应日志的index，不是clientId
	persister      *raft.Persister
	lastApplied    int
}

func (kv *KVServer) ExecCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if args.Op != OpGet && kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		debug.Debug(debug.KVServer, "S%v Duplicate Command args%v", kv.me, args)
		lastReply := kv.clientSessions[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		debug.Debug(debug.KVServer, "S%v Reply Command args %v, rply {%v %%v %v %v}", kv.me, args, reply.ClientId, reply.CommandId, reply.Err, len(reply.Value))
		kv.mu.Unlock()
		return
	}
	op := Command{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}
	kv.mu.Unlock()

	logIdx, _, isLeader := kv.rf.Start(op)
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
		reply.ClientId, reply.CommandId = result.ClientId, result.CommandId
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}
	kv.mu.Lock()
	// delete outdated notifyChan
	delete(kv.notifyChans, logIdx)
	debug.Debug(debug.KVServer, "S%v Reply Command args %v, rply {%v %v %v %v}", kv.me, args, reply.ClientId, reply.CommandId, reply.Err, len(reply.Value))
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				command := message.Command.(Command)

				if kv.lastApplied >= message.CommandIndex {
					debug.Debug(debug.KVServer, "S%v Recv Outdated Msg %v Due to newer Snapshot installed, Not Apply, lastApplied %v", kv.me, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply CommandReply
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = kv.clientSessions[command.ClientId].LastReply
					debug.Debug(debug.KVServer, "S%v Apply Duplicate Command %v Result {%v %v %v, %v}", kv.me, command, reply.ClientId, reply.CommandId, reply.Err, len(reply.Value))
				} else {
					reply = kv.applyLog(command)
					if command.Op != OpGet {
						kv.clientSessions[command.ClientId] = Session{command.CommandId, reply}
					}
				}

				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == message.CommandTerm && isLeader {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- reply
				}

				if kv.needSnapshot() {
					debug.Debug(debug.KVSnap, "S%d KVServer Take Snapshot, Msg %v, KVMAP[0]: %v",
						kv.me, message, len(kv.kvmap["0"]))
					snapshot := kv.takeSnapshot()
					kv.rf.Snapshot(message.CommandIndex, snapshot)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				debug.Debug(debug.KVSnap, "S%d KVServer Try Install Snapshot %d, KVMAP[0]: %v",
					kv.me, message.SnapshotIndex, len(kv.kvmap["0"]))
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.installSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
					debug.Debug(debug.KVSnap, "S%d KVServer Install Snapshot %d Finished, KVMAP[0]: %v",
						kv.me, message.SnapshotIndex, len(kv.kvmap["0"]))
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int) bool {
	// 不可能存在比lastCommandId还小；哪怕有，由于client已经发出commandId更大的command，因此client也已经不会接受该回复了
	session, ok := kv.clientSessions[clientId]
	return ok && commandId <= session.LastCommandId
}

func (kv *KVServer) applyLog(command Command) CommandReply {
	var reply CommandReply
	if command.Op == OpPut {
		kv.kvmap[command.Key] = command.Value
		reply.Value = kv.kvmap[command.Key]
		debug.Debug(debug.KVServer, "S%d Command %v Put Value to KVMAP[%v]: %v", kv.me, command, command.Key, len(kv.kvmap[command.Key]))
	} else if command.Op == OpAppend {
		kv.kvmap[command.Key] += command.Value
		reply.Value = kv.kvmap[command.Key]
		debug.Debug(debug.KVServer, "S%d Command %v Append Value to KVMAP[%v]: %v", kv.me, command, command.Key, len(kv.kvmap[command.Key]))
	} else if command.Op == OpGet {
		if val, ok := kv.kvmap[command.Key]; ok {
			reply.Value = val
		} else {
			reply.Value = ""
		}
		debug.Debug(debug.KVServer, "S%d Command %v Get Value to KVMAP[%v]: %v", kv.me, command, command.Key, len(reply.Value))
	}
	reply.ClientId, reply.CommandId, reply.Err = command.ClientId, command.CommandId, OK
	return reply
}

func (kv *KVServer) getNotifyChan(logIndex int) chan CommandReply {
	if _, ok := kv.notifyChans[logIndex]; !ok {
		kv.notifyChans[logIndex] = make(chan CommandReply, 1)
	}
	return kv.notifyChans[logIndex]
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvmap)
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
	var kvmap map[string]string
	var clientSessions map[int64]Session
	if d.Decode(&kvmap) != nil || d.Decode(&clientSessions) != nil {
		debug.Debug(debug.DError, "S%d KVServer Cannot Deserialize State", kv.me)
	} else {
		kv.kvmap, kv.clientSessions = kvmap, clientSessions
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmap = make(map[string]string)
	kv.clientSessions = make(map[int64]Session)
	kv.notifyChans = make(map[int]chan CommandReply)
	kv.persister = persister
	kv.lastApplied = 0

	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
