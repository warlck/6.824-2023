package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	// Type of the recevied job; can be MAP or REDUCE;
	// defined as const in coordinator.go
	TaskType uint

	// Reply fields relevant for map jobs
	FileName          string
	NReduce           int
	MapSequenceNumber int

	// Reply fields relevant for reduce jobs
	ReduceSequenceNumber int
}

type CompleteTaskArgs struct {
	TaskType             uint
	MapSequenceNumber    int
	ReduceSequenceNumber int
}

type CompleteTaskReply struct {
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
