package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

type WorkerState struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// Processes the task that was assigned by coordinator
// If the task type is MAP => runs mapf function on a fileName to obtain array of KeyValue types
// and stores them in the MxN files there M represents number of M tasks, N represents number of reduce tasks
// If the task type is REDUCE
func (w *WorkerState) ProcessTask(task RequestTaskReply) {
	if task.TaskType == MAP {
		fileName := task.FileName
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		kva := w.mapf(fileName, string(content))
		w.StoreKV(kva, task)
		CallCompleteTask(task)
	}

}

func (w *WorkerState) StoreKV(kva []KeyValue, task RequestTaskReply) {
	fileRefs := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)

	// Create temporary files for writing the output of mapf
	for i := 0; i < task.NReduce; i++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatal(err)
		}
		fileRefs[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFile)
	}

	// Write the output of mapf to corresponding temporary file
	for _, kv := range kva {
		reduceN := ihash(kv.Key) % task.NReduce
		enc := encoders[reduceN]
		if err := enc.Encode(&kv); err != nil {
			log.Fatal(err)
		}
	}

	// Rename the tempfiles to correct "mr-x-y" format, and close the file refences
	for i, _ := range fileRefs {
		destFileName := fmt.Sprintf("mr-%d-%d", task.MapSequenceNumber, i)
		os.Rename(fileRefs[i].Name(), destFileName)
		err := fileRefs[i].Close()
		if err != nil {
			log.Fatal(err)
		}
	}

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := WorkerState{mapf: mapf, reducef: reducef}

	for {
		task, err := CallRequestTask()
		if err != nil {
			break
		}

		if task.TaskType != WAIT {
			w.ProcessTask(task)
		}
		time.Sleep(time.Second * time.Duration(1))
	}
}

func CallRequestTask() (RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		debugf("reply %+v\n", reply)
		return reply, nil

	} else {
		debugf("call failed!\n")
		return reply, errors.New("Faild RPC request")
	}

}

func CallCompleteTask(task RequestTaskReply) {
	args := CompleteTaskArgs{
		TaskType:             task.TaskType,
		MapSequenceNumber:    task.MapSequenceNumber,
		ReduceSequenceNumber: task.ReduceSequenceNumber,
	}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		debugf("reply %+v\n", reply)

	} else {
		debugf("call failed!\n")
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
