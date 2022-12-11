package kvraft

type ErrType string

const (
	OK             ErrType = "OK"
	ErrNoKey       ErrType = "ErrNoKey"
	ErrWrongLeader ErrType = "ErrWrongLeader"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type CommandArgs struct {
	ClientId  int64
	CommandId int
	Key       string
	Value     string
	Op        OpType
}

type CommandReply struct {
	Err   ErrType
	Value string
}
