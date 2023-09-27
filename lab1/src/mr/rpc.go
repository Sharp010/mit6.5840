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
// 1.map worker
// 1.1 request a task
// 1.2 confirm a task
// handled filename

// 2.reduce worker
// 2.1 request a task
// 2.2 confirm a task
//

type RpcArgs struct {
	// -1 request 0 map 1 reduce
	TaskType int
	TaskNum  int
}

type RpcReply struct {
	// 0 map 1 reduce
	WorkType int
	TaskNum  int
	FileName string
	NReduce  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	//s += strconv.Itoa(1025)
	return s
}
