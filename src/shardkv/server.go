package shardkv

import (
	"6.824/debug"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"sync"
	"time"
)

const ClientRequestTimeout = 100 * time.Millisecond

type CommandType string

const (
	CmdOperation CommandType = "Operation"
	CmdConfig    CommandType = "Config"
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type CommandType
	Data interface{}
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
	mck            *shardctrler.Clerk
	shards         map[int]*Shard // shardId -> pointer of Shard obj
	clientSessions map[int64]Session
	notifyChans    map[int]chan CommandReply
	persister      *raft.Persister
	lastApplied    int
	currentCfg     *shardctrler.Config
}

func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	logIdx, _, isLeader := kv.rf.Start(command)
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

			if kv.lastApplied >= message.CommandIndex {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex

			var reply CommandReply
			command := message.Command.(Command)
			switch command.Type {
			case CmdOperation:
				operation := command.Data.(OperationArgs)
				reply = kv.applyOperation(&operation)
			case CmdConfig:
				nextCfg := command.Data.(shardctrler.Config)
				reply = kv.applyConfig(&nextCfg)
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
	labgob.Register(OperationArgs{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shards = make(map[int]*Shard)
	kv.clientSessions = make(map[int64]Session)
	kv.notifyChans = make(map[int]chan CommandReply)
	kv.persister = persister
	kv.lastApplied = 0
	kv.currentCfg = &shardctrler.Config{}

	for shardId := range kv.currentCfg.Shards {
		kv.shards[shardId] = NewShard()
	}

	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	go kv.applier()
	go kv.configUpdater()

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
	e.Encode(kv.shards)
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
	var shards map[int]*Shard
	var clientSessions map[int64]Session
	if d.Decode(&shards) != nil || d.Decode(&clientSessions) != nil {
		debug.Debug(debug.DError, "S%d KVServer Cannot Deserialize State", kv.me)
	} else {
		kv.shards, kv.clientSessions = shards, clientSessions
	}
}
