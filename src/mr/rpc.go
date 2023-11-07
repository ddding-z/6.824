package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

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

// Add your RPC definitions here.
type TaskType int

const (
	_ TaskType = iota
	MapTask
	ReduceTask
	WaitTask
	CompletedTask
)

type Phase int

const (
	_ Phase = iota
	Map
	Reduce
	Done
)

type Task struct {
	TaskId        int
	WorkerId      string
	TaskType      TaskType
	TaskInputFile string
	MapNum        int
	ReduceNum     int
	StartTime     time.Time
}

type ApplyRequest struct {
	WorkerId      string
	CompletedTask *Task
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
