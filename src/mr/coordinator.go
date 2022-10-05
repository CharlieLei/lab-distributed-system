package mr

import (
	"fmt"
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
		//fmt.Println("Acquire Task: set map task to running")
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
				fmt.Printf("Map Task %d time out!\n", taskID)
				delete(c.mapTaskRunningSet, taskID)
				c.mapTaskReadySet[taskID] = true
			}
		}(reply.TaskID)
		//fmt.Printf("	Acquire Task: map ready set size %d, map running set size %d\n", len(c.mapTaskReadySet), len(c.mapTaskRunningSet))
	} else if len(c.mapTaskRunningSet) > 0 {
		//fmt.Println("Acquire Task: still has workers running map task")
		// still has workers running map task
		reply.TaskType = WaitTask
	} else if len(c.reduceTaskReadySet) > 0 {
		//fmt.Println("Acquire Task: set reduce task to running")
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
				fmt.Printf("Reduce Task %d time out!\n", taskID)
				delete(c.reduceTaskRunningSet, taskID)
				c.reduceTaskReadySet[taskID] = true
			}
		}(reply.TaskID)
	} else if len(c.reduceTaskRunningSet) > 0 {
		//fmt.Println("Acquire Task: still has workers running reduce task")
		// still has workers running reduce task
		reply.TaskType = WaitTask
	} else {
		// all tasks have been done
		reply.TaskType = FinishTask
		fmt.Println("All tasks have been done, no need for acquiring task")
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == MapTask {
		fmt.Printf("Finish Map Task %d!\n", args.TaskID)
		if _, ok := c.mapTaskRunningSet[args.TaskID]; ok {
			fmt.Printf("	Finish Task: remove map task %d from running set\n", args.TaskID)
			// this map task is running before
			delete(c.mapTaskRunningSet, args.TaskID)
			c.mapTaskDoneSet[args.TaskID] = true
		} else {
			// this map task runs too long, coordinator has move this task to ready queue
			fmt.Printf("	Finish Task: map task %d time out?\n", args.TaskID)
			if _, ok := c.mapTaskReadySet[args.TaskID]; ok {
				fmt.Printf("	Finish Task: map task %d time out, map ready set size %d, map running set size %d\n", args.TaskID, len(c.mapTaskReadySet), len(c.mapTaskRunningSet))
				delete(c.mapTaskReadySet, args.TaskID)
			}
			c.mapTaskDoneSet[args.TaskID] = true
		}
		fmt.Printf("	Finish Task: map ready set size %d, map running set size %d\n", len(c.mapTaskReadySet), len(c.mapTaskRunningSet))
		if len(c.mapTaskReadySet) == 0 && len(c.mapTaskRunningSet) == 0 {
			// all map tasks have been done, now can start reduce tasks
			for i := 0; i < c.numReducer; i++ {
				c.reduceTaskReadySet[i] = true
			}
		}
	} else {
		fmt.Printf("Finish Reduce Task %d!\n", args.TaskID)
		_, ok := c.reduceTaskRunningSet[args.TaskID]
		if ok {
			// this reduce task is running before
			delete(c.reduceTaskRunningSet, args.TaskID)
			c.reduceTaskDoneSet[args.TaskID] = true
		} else {
			// this reduce task runs too long, coordinator has move this task to ready queue
			if _, ok := c.reduceTaskReadySet[args.TaskID]; ok {
				fmt.Printf("	Finish Task: reduce task %d time out, reduce ready set size %d, reduce running set size %d\n", args.TaskID, len(c.reduceTaskReadySet), len(c.reduceTaskRunningSet))
				delete(c.reduceTaskReadySet, args.TaskID)
			}
			c.reduceTaskDoneSet[args.TaskID] = true
		}
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
	fmt.Println("Coordinator initialized!")
	fmt.Printf("Coordinator initialized!, ready set size: %d\n", len(c.mapTaskReadySet))

	c.server()
	return &c
}
