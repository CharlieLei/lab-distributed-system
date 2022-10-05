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
		if ok := call("Coordinator.AcquireTask", &args, &reply); !ok {
			log.Fatal("call rpc AcquireTask Failed")
		}
		if reply.TaskType == MapTask {
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open map task file %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read map task file %v", filename)
			}
			if err := file.Close(); err != nil {
				log.Fatalf("cannot close map task file %v", filename)
			}
			kva := mapf(filename, string(content))
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
				interFile, err := os.CreateTemp(".", interFilename)
				if err != nil {
					log.Fatalf("cannot create temp file: %v", err)
				}
				encoder := json.NewEncoder(interFile)
				for _, kv := range outputArrays[i] {
					if err := encoder.Encode(&kv); err != nil {
						log.Fatalf("cannot encode ")
					}
				}
				if err := interFile.Close(); err != nil {
					log.Fatalf("cannot close map task intermediate file %v", interFilename)
				}
				// change intermediate file name atomically
				if err := os.Rename(interFile.Name(), interFilename); err != nil {
					log.Fatalf("Map Task: cannot rename temp file: %v", err)
				}
			}
			finishArgs := FinishTaskArgs{}
			finishReply := FinishTaskReply{}
			finishArgs.TaskType = MapTask
			finishArgs.TaskID = reply.TaskID
			if ok := call("Coordinator.FinishTask", &finishArgs, &finishReply); !ok {
				log.Fatal("call rpc FinishTask Failed")
			}
		} else if reply.TaskType == ReduceTask {
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
				if err := file.Close(); err != nil {
					log.Fatalf("cannot close reduce task input file %v", filename)
				}
			}

			var outputKVA []KeyValue
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
			if err := tempFile.Close(); err != nil {
				log.Fatalf("cannot close reduce task output file %v", outputFilename)
			}
			if err := os.Rename(tempFile.Name(), outputFilename); err != nil {
				log.Fatalf("Reduce Task: cannot rename temp file: %v", err)
			}
			finishArgs := FinishTaskArgs{}
			finishReply := FinishTaskReply{}
			finishArgs.TaskType = ReduceTask
			finishArgs.TaskID = reply.TaskID
			if ok := call("Coordinator.FinishTask", &finishArgs, &finishReply); !ok {
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
