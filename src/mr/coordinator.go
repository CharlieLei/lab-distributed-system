package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTaskReadySet      map[int]bool
	mapTaskRunningSet    map[int]bool
	mapTaskDoneSet       map[int]bool
	reduceTaskReadySet   map[int]bool
	reduceTaskRunningSet map[int]bool
	reduceTaskDoneSet    map[int]bool

	filenames  []string
	numReducer int

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AcquireTask(args *AcquireTaskArgs, reply *AcquireTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.mapTaskReadySet) > 0 {
		// still has map tasks to be assigned to worker
		for i := range c.mapTaskReadySet {
			reply.TaskType = MapTask
			reply.TaskID = i
			reply.Filename = c.filenames[i]
			reply.NumReduceTask = c.numReducer
			delete(c.mapTaskReadySet, i)
			c.mapTaskRunningSet[i] = true
			break
		}
		go func(taskID int) {
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			defer c.mutex.Unlock()
			_, doneok := c.mapTaskDoneSet[taskID]
			_, runningok := c.mapTaskRunningSet[taskID]
			if !doneok && runningok {
				delete(c.mapTaskRunningSet, taskID)
				c.mapTaskReadySet[taskID] = true
			}
		}(reply.TaskID)
	} else if len(c.mapTaskRunningSet) > 0 {
		// still has workers running map task
		reply.TaskType = WaitTask
	} else if len(c.reduceTaskReadySet) > 0 {
		// still has reduce tasks to be assigned to worker
		for i := range c.reduceTaskReadySet {
			reply.TaskType = ReduceTask
			reply.TaskID = i
			reply.NumMapTask = len(c.filenames)
			delete(c.reduceTaskReadySet, i)
			c.reduceTaskRunningSet[i] = true
			break
		}
		go func(taskID int) {
			time.Sleep(10 * time.Second)
			c.mutex.Lock()
			defer c.mutex.Unlock()
			_, doneok := c.reduceTaskDoneSet[taskID]
			_, runningok := c.reduceTaskRunningSet[taskID]
			if !doneok && runningok {
				delete(c.reduceTaskRunningSet, taskID)
				c.reduceTaskReadySet[taskID] = true
			}
		}(reply.TaskID)
	} else if len(c.reduceTaskRunningSet) > 0 {
		// still has workers running reduce task
		reply.TaskType = WaitTask
	} else {
		// all tasks have been done
		reply.TaskType = FinishTask
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == MapTask {
		if _, ok := c.mapTaskRunningSet[args.TaskID]; ok {
			// this map task is running before
			delete(c.mapTaskRunningSet, args.TaskID)
		} else if _, ok := c.mapTaskReadySet[args.TaskID]; ok {
			// this map task runs too long, coordinator has move this task to ready queue
			delete(c.mapTaskReadySet, args.TaskID)
		}
		c.mapTaskDoneSet[args.TaskID] = true
		if len(c.mapTaskReadySet) == 0 && len(c.mapTaskRunningSet) == 0 {
			// all map tasks have been done, now can start reduce tasks
			for i := 0; i < c.numReducer; i++ {
				c.reduceTaskReadySet[i] = true
			}
		}
	} else {
		if _, ok := c.reduceTaskRunningSet[args.TaskID]; ok {
			// this reduce task is running before
			delete(c.reduceTaskRunningSet, args.TaskID)
		} else if _, ok := c.reduceTaskReadySet[args.TaskID]; ok {
			// this reduce task runs too long, coordinator has move this task to ready queue
			delete(c.reduceTaskReadySet, args.TaskID)
		}
		c.reduceTaskDoneSet[args.TaskID] = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.mapTaskReadySet) == 0 && len(c.mapTaskRunningSet) == 0 && len(c.reduceTaskReadySet) == 0 && len(c.reduceTaskRunningSet) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initialize coordinator
	c.mapTaskReadySet = make(map[int]bool)
	c.mapTaskRunningSet = make(map[int]bool)
	c.mapTaskDoneSet = make(map[int]bool)
	c.reduceTaskReadySet = make(map[int]bool)
	c.reduceTaskRunningSet = make(map[int]bool)
	c.reduceTaskDoneSet = make(map[int]bool)
	c.filenames = files
	c.numReducer = nReduce
	for i := 0; i < len(c.filenames); i++ {
		c.mapTaskReadySet[i] = true
	}

	c.server()
	return &c
}
