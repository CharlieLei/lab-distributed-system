package kvraft

import "time"

const ClientRequestTimeout = 100 * time.Millisecond

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

type Command struct {
	*CommandArgs
}

type Session struct {
	LastSequenceNum int
	LastReply       *CommandReply
}

type CommandArgs struct {
	ClientId    int64
	SequenceNum int // 就是command的id
	Op          OpType
	Key         string
	Value       string
}

func (args *CommandArgs) isReadOnly() bool {
	return args.Op == OpGet
}

type CommandReply struct {
	Err   ErrType
	Value string
}
