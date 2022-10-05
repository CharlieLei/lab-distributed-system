package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	WaitTask   TaskType = 2
	FinishTask TaskType = 3
)

type AcquireTaskArgs struct {
}

type AcquireTaskReply struct {
	TaskType      TaskType // task type = {map, reduce, wait}
	TaskID        int      // task id
	Filename      string   // file read by map function
	NumMapTask    int      // number of map tasks
	NumReduceTask int      // number of reduce tasks
}

type FinishTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
