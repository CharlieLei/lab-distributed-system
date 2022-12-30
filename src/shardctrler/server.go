package shardctrler

import (
	"6.824/debug"
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const ClientRequestTimeout = 100 * time.Millisecond

type Command struct {
	Args *CommandArgs
}

type Session struct {
	LastCommandId int
	LastReply     CommandReply
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs        []Config // indexed by config num
	clientSessions map[int64]Session
	notifyChans    map[int]chan CommandReply // 键值是对应日志的index，不是clientId
	lastApplied    int
}

func (sc *ShardCtrler) ExecCommand(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	if args.Op != OpQuery && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
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
		args := message.Command.(Command).Args

		if sc.lastApplied >= message.CommandIndex {
			sc.mu.Unlock()
			continue
		}
		sc.lastApplied = message.CommandIndex

		var reply CommandReply
		if args.Op != OpQuery && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
			reply = sc.clientSessions[args.ClientId].LastReply
		} else {
			sc.applyLog(args, &reply)
			if args.Op != OpQuery {
				sc.clientSessions[args.ClientId] = Session{args.CommandId, reply}
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

func (sc *ShardCtrler) applyLog(args *CommandArgs, reply *CommandReply) {
	switch args.Op {
	case OpJoin:
		reply.Err = sc.Join(args.Servers)
	case OpLeave:
		reply.Err = sc.Leave(args.GIDs)
	case OpMove:
		reply.Err = sc.Move(args.Shard, args.GID)
	case OpQuery:
		reply.Err, reply.Config = sc.Query(args.Num)
	default:
		panic("WRONG CommandArgs OpType")
	}
	debug.Debug(debug.KVServer, "S%d Command %v, configs %v, rply %v",
		sc.me, args, sc.configs, reply)
}

func (sc *ShardCtrler) Join(groups map[int][]string) ErrType {
	// Your code here.
	newCfg := sc.configs[len(sc.configs)-1].copy()
	newCfg.Num++
	for groupId, shards := range groups {
		newCfg.Groups[groupId] = shards
	}
	newCfg.reAllocateGid()
	sc.configs = append(sc.configs, newCfg)
	return OK
}

func (sc *ShardCtrler) Leave(gids []int) ErrType {
	// Your code here.
	newCfg := sc.configs[len(sc.configs)-1].copy()
	newCfg.Num++
	for _, groupId := range gids {
		delete(newCfg.Groups, groupId)
	}
	newCfg.reAllocateGid()
	sc.configs = append(sc.configs, newCfg)
	return OK
}

func (sc *ShardCtrler) Move(shard int, gid int) ErrType {
	// Your code here.
	newCfg := sc.configs[len(sc.configs)-1].copy()
	newCfg.Num++
	newCfg.Shards[shard] = gid
	sc.configs = append(sc.configs, newCfg)
	return OK
}

func (sc *ShardCtrler) Query(num int) (ErrType, Config) {
	// Your code here.
	queryIdx := num
	if num == -1 || num >= len(sc.configs) {
		queryIdx = len(sc.configs) - 1
	}
	return OK, sc.configs[queryIdx]
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
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = make([]Config, 1)
	sc.clientSessions = make(map[int64]Session)
	sc.notifyChans = make(map[int]chan CommandReply)
	sc.lastApplied = 0

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) getNotifyChan(logIndex int) chan CommandReply {
	if _, ok := sc.notifyChans[logIndex]; !ok {
		sc.notifyChans[logIndex] = make(chan CommandReply, 1)
	}
	return sc.notifyChans[logIndex]
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int) bool {
	session, ok := sc.clientSessions[clientId]
	return ok && commandId <= session.LastCommandId
}
