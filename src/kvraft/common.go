package kvraft

type ErrType string

const (
	OK             ErrType = "OK"
	ErrWrongLeader ErrType = "ErrWrongLeader"
	ErrTimeout     ErrType = "ErrTimeout"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type CommandArgs struct {
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

func (args *CommandArgs) isReadOnly() bool {
	return args.Op == OpGet
}
