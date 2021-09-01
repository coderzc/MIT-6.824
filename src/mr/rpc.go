package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task *Task
	Done bool
}

type ReportTaskArgs struct {
	Succeeded bool
	TaskIndex int
	WorkerId  int
	Phase     TaskPhase
}

type ReportTaskReply struct {
}

type RegisterArgs struct {
	WorkerId int
}

type RegisterReply struct {
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
