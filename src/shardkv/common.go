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
	OK             ErrType = "OK"
	ErrNoKey       ErrType = "ErrNoKey"
	ErrTimeout     ErrType = "ErrTimeout"
	ErrWrongGroup  ErrType = "ErrWrongGroup"
	ErrWrongLeader ErrType = "ErrWrongLeader"
	ErrOutDated    ErrType = "ErrOutDated"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type OperationArgs struct {
	ClientId    int64
	SequenceNum int // 就是command的id
	Op          OpType
	Key         string
	Value       string
}

type CommandReply struct {
	Err   ErrType
	Value string
}

func (args *OperationArgs) isReadOnly() bool {
	return args.Op == OpGet
}
