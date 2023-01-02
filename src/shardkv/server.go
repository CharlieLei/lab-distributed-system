package shardkv

import (
	"6.824/debug"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"sync"
	"time"
)

const ClientRequestTimeout = 100 * time.Millisecond

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Args *CommandArgs
}

type Session struct {
	LastSequenceNum int
	LastReply       CommandReply
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvmap          map[string]string
	clientSessions map[int64]Session
	notifyChans    map[int]chan CommandReply
	persister      *raft.Persister
	lastApplied    int
}

func (kv *ShardKV) ExecCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
		lastReply := kv.clientSessions[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
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
	ch := kv.getNotifyChan(logIdx)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.notifyChans, logIdx)
	kv.mu.Unlock()
}

func (kv *ShardKV) applier() {
	for message := range kv.applyCh {
		if message.CommandValid {
			kv.mu.Lock()
			args := message.Command.(Command).Args

			if kv.lastApplied >= message.CommandIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex

			var reply CommandReply
			if !args.isReadOnly() && kv.isDuplicateRequest(args.ClientId, args.SequenceNum) {
				reply = kv.clientSessions[args.ClientId].LastReply
			} else {
				reply = kv.applyLog(args)
				if !args.isReadOnly() {
					kv.clientSessions[args.ClientId] = Session{args.SequenceNum, reply}
				}
			}

			currentTerm, isLeader := kv.rf.GetState()
			if currentTerm == message.CommandTerm && isLeader {
				ch := kv.getNotifyChan(message.CommandIndex)
				ch <- reply
			}

			if kv.needSnapshot() {
				snapshot := kv.takeSnapshot()
				kv.rf.Snapshot(message.CommandIndex, snapshot)
			}
			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				kv.installSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
			}
			kv.mu.Unlock()
		} else {
			panic("WRONG Message Valid Situation")
		}
	}
}

func (kv *ShardKV) applyLog(args *CommandArgs) CommandReply {
	var reply CommandReply
	switch args.Op {
	case OpPut:
		kv.kvmap[args.Key] = args.Value
		reply.Value = kv.kvmap[args.Key]
	case OpAppend:
		kv.kvmap[args.Key] += args.Value
		reply.Value = kv.kvmap[args.Key]
	case OpGet:
		if val, ok := kv.kvmap[args.Key]; ok {
			reply.Value = val
		} else {
			reply.Value = ""
		}
	}
	reply.Err = OK
	return reply
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmap = make(map[string]string)
	kv.clientSessions = make(map[int64]Session)
	kv.notifyChans = make(map[int]chan CommandReply)
	kv.persister = persister
	kv.lastApplied = 0

	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	go kv.applier()

	return kv
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, sequenceNum int) bool {
	// 不可能存在比lastCommandId还小；哪怕有，由于client已经发出commandId更大的command，因此client也已经不会接受该回复了
	session, ok := kv.clientSessions[clientId]
	return ok && sequenceNum <= session.LastSequenceNum
}

func (kv *ShardKV) getNotifyChan(logIndex int) chan CommandReply {
	if _, ok := kv.notifyChans[logIndex]; !ok {
		kv.notifyChans[logIndex] = make(chan CommandReply, 1)
	}
	return kv.notifyChans[logIndex]
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate
}

func (kv *ShardKV) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvmap)
	e.Encode(kv.clientSessions)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
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
