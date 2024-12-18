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

type GetTask struct {
	X int
}

type Taskcomplete struct {
	Do_what int
	Mrnum	int
	Outputfile []string
}

type Reply struct {
	Task Task
	// 0 for map , 1 for reduce
	Do_what   int
	Need_exit int
	Nreduce   int
	Need_delete bool
	Need_wait   bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
