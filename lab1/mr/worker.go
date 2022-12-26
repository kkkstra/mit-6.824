package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// periodically request tasks from the coordinator
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok || reply.TaskType == 0 {
			// failed to contact the coordinator, so the job is done
			break
		}

		switch reply.TaskType {
		case 1:
			// map task

			// open and read the file
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			// call mapf and generate intermeiate keys
			intermediate := mapf(reply.Filename, string(content))

			// divide the intermediate keys into buckets
			buckets := make([][]KeyValue, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				reduceNumber := ihash(kv.Key) % reply.NReduce
				buckets[reduceNumber] = append(buckets[reduceNumber], kv)
			}

			// write into the intermediate files
			for i := 0; i < reply.NReduce; i++ {
				oname := "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(i)
				ofile, err := os.CreateTemp("", oname+"*")
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}

			// notify the coordinator that the task is done
			args = WorkerArgs{reply.TaskType, reply.MapTaskId}
			reply = WorkerReply{}
			call("Coordinator.MapTaskFinished", &args, &reply)
		case 2:
			// reduce task

		}

		time.Sleep(time.Second)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
