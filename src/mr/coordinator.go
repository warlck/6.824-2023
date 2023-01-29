package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Constant values defined to help with the implementation of mapreduce coordinator and workers
const (
	// The job (map or reduce) has not started yet
	statusNotStarted = 0
	// The job (map or reduce) has started and is currently being processed by worker
	statusPending = 1
	// The job (map or reduce) has been statusCompleted by the worker process
	statusCompleted = 2

	// These values are uused to  signal the worker if the job type is map or reduce
	MAP    = 10
	REDUCE = 20
	WAIT   = 30
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
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	reply.NReduce = c.nReduce
	taskType := MAP

	c.mu.Lock()
	if c.completedMapJobs == len(c.files) {
		taskType = REDUCE
	}

	if taskType == MAP {
		for i := 0; i < len(c.files); i++ {
			fileName := c.files[i]
			status := c.mapJobsStatus[fileName]

			if status == statusNotStarted {
				reply.FileName = fileName
				reply.TaskType = MAP
				reply.MapSequenceNumber = i
				c.mapJobsStatus[fileName] = statusPending
				// Start a 10 timer to check status of the job in 10 secs,
				// Return the status of the task to notStarted if task is not completed in 10 sec
				// this will make the  task available  to be scheduled with next worker
				go c.checkStatusOfTask(MAP, i)
				break
			}
		}
		// Check if MAP job has been assigned after traversing the mapJobsStatus
		// If no job has been assigned => MAP tasks are pending completion, need to wait
		if reply.TaskType != MAP {
			reply.TaskType = WAIT
		}

	}

	if taskType == REDUCE {
		for i, status := range c.reduceJobsStatus {
			if status == statusNotStarted {
				reply.TaskType = REDUCE
				reply.ReduceSequenceNumber = i
				c.reduceJobsStatus[i] = statusPending
				// Start a 10 seconds timer to check status of the job in 10 secs,
				// Return the status of the task to notStarted if task is not completed in 10 sec
				// this will make the  task available  to be scheduled with next worker
				go c.checkStatusOfTask(REDUCE, i)
				break
			}
		}

		// Check if REDUCE teask has been assigned after traversing the reduceJobsStatus
		// If no job has been assigned => REDUCE tasks are pending completion, need to wait
		if reply.TaskType != REDUCE {
			reply.TaskType = WAIT
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	if args.TaskType == MAP {
		fileName := c.files[args.MapSequenceNumber]
		c.mapJobsStatus[fileName] = statusCompleted
		c.completedMapJobs += 1
	} else if args.TaskType == REDUCE {
		reduceN := args.ReduceSequenceNumber
		c.reduceJobsStatus[reduceN] = statusCompleted
		c.completedReduceJobs += 1
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) checkStatusOfTask(taskType uint, sequenceNumber int) {
	time.Sleep(time.Second * time.Duration(10))
	c.mu.Lock()
	if taskType == MAP {
		fileName := c.files[sequenceNumber]
		if c.mapJobsStatus[fileName] == statusPending {
			c.mapJobsStatus[fileName] = statusNotStarted
		}
	} else if taskType == REDUCE {
		if c.reduceJobsStatus[sequenceNumber] == statusPending {
			c.reduceJobsStatus[sequenceNumber] = statusNotStarted
		}
	}
	c.mu.Unlock()

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
		c.mapJobsStatus[file] = statusNotStarted
	}

	c.server()
	return &c
}
