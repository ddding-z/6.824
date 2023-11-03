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
// 任务类型
type Type int

const (
	_ Type = iota
	MapTask
	ReduceTask
	WaitTask // 任务已分发结束，但仍在运行
	ExitTask
)

// 任务状态
type Status int

const (
	_ Status = iota
	Waiting
	Working
	Done
)

type Task struct {
	TaskId       int
	TaskType     Type
	NReduce      int
	TaskStatus   Status
	MapInputFile string
	//FileSlice    []string
	DeadLine time.Time
}

// apply for task rpc
type ApplyRequest struct{}

type ApplyResponse struct {
	JobType  Type
	FileName string
	NReduce  int
	TaskId   int
}

type ReportRequest struct {
	JobType    Type
	TaskId     int
	TaskStatus Status
}

type ReportResponse struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
