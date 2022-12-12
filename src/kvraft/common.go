package kvraft

type ErrType string

const (
	OK             ErrType = "OK"
	ErrNoKey       ErrType = "ErrNoKey"
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
	ClientId  int64
	CommandId int
	Op        OpType
	Key       string
	Value     string
}

type CommandReply struct {
	ClientId  int64
	CommandId int
	Err       ErrType
	Value     string
}
