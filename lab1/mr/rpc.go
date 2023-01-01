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

// type of task
const (
	ExitTask    = 0
	MapTask     = 1
	ReduceTask  = 2
	WaitingTask = 3
)

// state of task
const (
	WaitingState = 0
	WorkingState = 1
	DoneState    = 2
)

// phase of work
const (
	ExitPhase   = 0
	MapPhase    = 1
	ReducePhase = 2
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Task struct {
	TaskType int
	TaskId   int
	Filename string
}

type WorkerArgs struct {
	TaskType int
	TaskId   int
}

type WorkerReply struct {
	TaskType int
	NReduce  int
	NMap     int

	TaskId   int
	Filename string // for map task
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
