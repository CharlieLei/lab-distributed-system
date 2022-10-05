package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		args := AcquireTaskArgs{}
		reply := AcquireTaskReply{}
		ok := call("Coordinator.AcquireTask", &args, &reply)
		if !ok {
			log.Fatal("call rpc AcquireTask Failed")
		}
		if reply.TaskType == MapTask {
			fmt.Printf("Start to Do MAP Task %v!\n", reply.TaskID)
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open map task file %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read map task file %v", filename)
			}
			file.Close()
			//fmt.Printf("	MAP Task %v: start to execute mapf\n", reply.TaskID)
			kva := mapf(filename, string(content))
			//fmt.Printf("	MAP Task %v: finish to execute mapf\n", reply.TaskID)
			outputArrays := make([][]KeyValue, reply.NumReduceTask)
			for i := 0; i < reply.NumReduceTask; i++ {
				outputArrays[i] = []KeyValue{}
			}
			for _, kv := range kva {
				idx := ihash(kv.Key) % reply.NumReduceTask
				outputArrays[idx] = append(outputArrays[idx], kv)
			}
			for i := 0; i < reply.NumReduceTask; i++ {
				interFilename := fmt.Sprintf("mr-inter-%d-%d", reply.TaskID, i)
				//fmt.Printf("	MAP Task %v: start to create intermediate file %v\n", reply.TaskID, interFilename)
				interFile, err := os.CreateTemp(".", interFilename)
				if err != nil {
					log.Fatalf("cannot create temp file: %v", err)
				}
				encoder := json.NewEncoder(interFile)
				for _, kv := range outputArrays[i] {
					err := encoder.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode ")
					}
				}
				interFile.Close()
				// change intermediate file name atomically
				if err := os.Rename(interFile.Name(), interFilename); err != nil {
					log.Fatalf("Map Task: cannot rename temp file: %v", err)
				}
				//fmt.Printf("	MAP Task %v: finish to create intermediate file %v\n", reply.TaskID, interFilename)
			}
			finishArgs := FinishTaskArgs{}
			finishReply := FinishTaskReply{}
			finishArgs.TaskType = MapTask
			finishArgs.TaskID = reply.TaskID
			ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
			if !ok {
				log.Fatal("call rpc FinishTask Failed")
			}
		} else if reply.TaskType == ReduceTask {
			fmt.Printf("Start to Do Reduce Task %v!\n", reply.TaskID)
			interDataMap := make(map[string][]string)
			for i := 0; i < reply.NumMapTask; i++ {
				filename := fmt.Sprintf("mr-inter-%d-%d", i, reply.TaskID)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open reduce task file %v", filename)
				}
				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					interDataMap[kv.Key] = append(interDataMap[kv.Key], kv.Value)
				}
				file.Close()
			}

			outputKVA := []KeyValue{}
			for key, vals := range interDataMap {
				kv := KeyValue{key, reducef(key, vals)}
				outputKVA = append(outputKVA, kv)
			}

			outputFilename := fmt.Sprintf("mr-out-%d", reply.TaskID)
			tempFile, err := os.CreateTemp(".", outputFilename)
			if err != nil {
				log.Fatalf("cannot create temp file: %v", err)
			}
			for _, kv := range outputKVA {
				fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
			}
			tempFile.Close()
			if err := os.Rename(tempFile.Name(), outputFilename); err != nil {
				log.Fatalf("Reduce Task: cannot rename temp file: %v", err)
			}
			finishArgs := FinishTaskArgs{}
			finishReply := FinishTaskReply{}
			finishArgs.TaskType = ReduceTask
			finishArgs.TaskID = reply.TaskID
			ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
			if !ok {
				log.Fatal("call rpc FinishTask Failed")
			}
		} else if reply.TaskType == WaitTask {
			time.Sleep(1 * time.Second)
		} else if reply.TaskType == FinishTask {
			break
		} else {
			fmt.Println("undefined task type")
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
