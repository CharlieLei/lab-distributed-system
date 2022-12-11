package kvraft

import (
	"6.824/debug"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
)

type Command struct {
	Key       string
	Value     string
	Op        OpType
	ClientId  int64
	CommandId int
}

type Session struct {
	lastCommandId int
	lastReply     *CommandReply
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
	notifyChans    map[int64]chan *CommandReply
}

func (kv *KVServer) ExecCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := kv.clientSessions[args.ClientId].lastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
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

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	debug.Debug(debug.KVServer, "S%v Command Has Started args%v, rply%v", kv.me, args, reply)
	ch := kv.getNotifyChan(args.ClientId)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Err, reply.Value = result.Err, result.Value
	}
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				command := message.Command.(Command)
				debug.Debug(debug.KVServer, "S%v Apply Command%v", kv.me, command)
				reply := kv.applyLog(command)
				kv.clientSessions[command.ClientId] = Session{command.CommandId, reply}
				ch := kv.getNotifyChan(command.ClientId)
				kv.mu.Unlock()
				ch <- reply
			}
		}
	}
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int) bool {
	// 不可能存在比lastCommandId还小；哪怕有，由于client已经发出commandId更大的command，因此client也已经不会接受该回复了
	session, ok := kv.clientSessions[clientId]
	return ok && commandId <= session.lastCommandId
}

func (kv *KVServer) applyLog(command Command) *CommandReply {
	var reply CommandReply
	if command.Op == OpPut {
		kv.kvmap[command.Key] = command.Value
	} else if command.Op == OpAppend {
		kv.kvmap[command.Key] += command.Value
	} else if command.Op == OpGet {
		if val, ok := kv.kvmap[command.Key]; ok {
			reply.Value = val
		} else {
			reply.Value = ""
		}
	}
	reply.Err = OK
	return &reply
}

func (kv *KVServer) getNotifyChan(clientId int64) chan *CommandReply {
	if _, ok := kv.notifyChans[clientId]; !ok {
		kv.notifyChans[clientId] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[clientId]
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
	kv.notifyChans = make(map[int64]chan *CommandReply)

	// You may need initialization code here.

	go kv.applier()

	return kv
}
