package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Constant values defined to help with the implementation of mapreduce coordinator and workers
const (
	// The job (map or reduce) has not started yet
	notStarted = 0
	// The job (map or reduce) has started and is currently being processed by worker
	pending = 1
	// The job (map or reduce) has been completed by the worker process
	completed = 2

	// These values are uused to  signal the worker if the job type is map or reduce
	MAP    = 10
	REDUCE = 20
)

type Coordinator struct {
	files []string

	// Tracks the status of map jobs
	mapJobsStatus map[string]uint
	// Value that will track the number of completed map jobs
	// The value will be helpful to determine if all the map jobs have been completed and reduce jobs
	// can be assigned to workers
	completedMapJobs int

	// Tracks the status of reduce jobs
	reduceJobsStatus []uint
	// Counts tbe number of completed reduce jobs
	// Will be helpful for determining if all jobs are done
	completedReduceJobs int

	// nReduce - number of reduce jobs that coordinator needs to create
	nReduce int
	// nFiles - number of files used
	nFiles int

	// Mutex to protect Coordinator's shared state during concurrent access
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler that workers use to request for a new task from the coordinator
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) {
	reply.nReduce = c.nReduce
	taskType := MAP

	c.mu.Lock()
	if c.completedMapJobs == c.nFiles {
		taskType = REDUCE
	}

	if taskType == MAP {
		for filename, status := range c.mapJobsStatus {
			if status == notStarted {
				reply.fileName = filename
				reply.task = MAP
				c.mapJobsStatus[filename] = pending
				// TODO: Start a 10 timer to check status of the job in 10 secs,
				// Return the status of the task to noStarted if task is not completed in 10 sec
				break
			}
		}
	}
	c.mu.Unlock()
	debugf("c.mapsJobsStatus: %v\n ", c.mapJobsStatus)
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
	ret := false

	c.mu.Lock()
	if c.completedReduceJobs == c.nReduce {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:            files,
		mapJobsStatus:    make(map[string]uint),
		reduceJobsStatus: make([]uint, nReduce),
		mu:               sync.Mutex{},
		nReduce:          nReduce,
		nFiles:           len(files),
	}

	for _, file := range files {
		c.mapJobsStatus[file] = notStarted
	}

	c.server()
	return &c
}
