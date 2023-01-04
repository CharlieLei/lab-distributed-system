package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type ErrType string

const (
	OK               ErrType = "OK"
	ErrNoKey         ErrType = "ErrNoKey"
	ErrTimeout       ErrType = "ErrTimeout"
	ErrWrongGroup    ErrType = "ErrWrongGroup"
	ErrWrongLeader   ErrType = "ErrWrongLeader"
	ErrOutDated      ErrType = "ErrOutDated"
	ErrShardNotReady ErrType = "ErrShardNotReady"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type CommandType string

const (
	CmdOperation    CommandType = "Operation"
	CmdConfig       CommandType = "Config"
	CmdInsertShards CommandType = "InsertShards"
)

type Command struct {
	Type CommandType
	Data interface{}
}

type Session struct {
	LastSequenceNum int
	LastReply       CommandReply
}

func (s *Session) deepcopy() Session {
	reply := CommandReply{s.LastReply.Err, s.LastReply.Value}
	return Session{s.LastSequenceNum, reply}
}

type OperationArgs struct {
	ClientId    int64
	SequenceNum int // 就是command的id
	Op          OpType
	Key         string
	Value       string
}

func (args *OperationArgs) isReadOnly() bool {
	return args.Op == OpGet
}

type CommandReply struct {
	Err   ErrType
	Value string
}

type ShardMigrationArgs struct {
	ConfigNum int
	ShardIds  []int
}

type ShardMigrationReply struct {
	Err            ErrType
	ConfigNum      int
	ShardsKV       map[int]map[string]string
	ClientSessions map[int64]Session
}
