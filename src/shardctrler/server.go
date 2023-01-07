package shardctrler

import (
	"6.824/debug"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stateMachine   *ConfigStateMachine
	clientSessions map[int64]Session
	notifyChans    map[int]chan *CommandReply // 键值是对应日志的index，不是clientId
	lastApplied    int
}

func (sc *ShardCtrler) ExecCommand(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if !args.isReadOnly() && sc.isDuplicateRequest(args.ClientId, args.SequenceNum) {
		lastReply := sc.clientSessions[args.ClientId].LastReply
		reply.Err, reply.Config = lastReply.Err, lastReply.Config
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	logIdx, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(logIdx)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err, reply.Config = result.Err, result.Config
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}
	sc.mu.Lock()
	delete(sc.notifyChans, logIdx)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) applier() {
	for message := range sc.applyCh {
		if !message.CommandValid {
			continue
		}
		sc.mu.Lock()
		command := message.Command.(Command)

		if sc.lastApplied >= message.CommandIndex {
			sc.mu.Unlock()
			continue
		}
		sc.lastApplied = message.CommandIndex

		var reply *CommandReply
		if !command.isReadOnly() && sc.isDuplicateRequest(command.ClientId, command.SequenceNum) {
			reply = sc.clientSessions[command.ClientId].LastReply
		} else {
			reply = sc.applyLog(&command)
			if !command.isReadOnly() {
				sc.clientSessions[command.ClientId] = Session{command.SequenceNum, reply}
			}
		}

		currentTerm, isLeader := sc.rf.GetState()
		if currentTerm == message.CommandTerm && isLeader {
			ch := sc.getNotifyChan(message.CommandIndex)
			ch <- reply
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) applyLog(command *Command) *CommandReply {
	var reply CommandReply
	switch command.Op {
	case OpJoin:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		reply.Err, reply.Config = sc.stateMachine.Query(command.Num)
	default:
		panic("WRONG CommandArgs OpType")
	}
	debug.Debug(debug.CTServer, "S%d Command %v, StateMachine %v, rply %v",
		sc.me, command, sc.stateMachine, reply)
	return &reply
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})

	applyCh := make(chan raft.ApplyMsg)
	sc := &ShardCtrler{
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		stateMachine:   NewConfigStateMachine(),
		clientSessions: make(map[int64]Session),
		notifyChans:    make(map[int]chan *CommandReply),
		lastApplied:    0,
	}
	go sc.applier()

	return sc
}

func (sc *ShardCtrler) getNotifyChan(logIndex int) chan *CommandReply {
	if _, ok := sc.notifyChans[logIndex]; !ok {
		sc.notifyChans[logIndex] = make(chan *CommandReply, 1)
	}
	return sc.notifyChans[logIndex]
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, sequenceNum int) bool {
	session, ok := sc.clientSessions[clientId]
	return ok && sequenceNum <= session.LastSequenceNum
}
