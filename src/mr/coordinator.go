package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files    []string
	nReduce  int
	nMap     int
	tasks    map[Type][]*Task
	applyCh  chan applyMsg
	reportCh chan reportMsg
	doneCh   chan struct{}
}
type applyMsg struct {
	response *ApplyResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

const (
	Timeout = 10
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Apply4Task(request *ApplyRequest, response *ApplyResponse) error {
	msg := applyMsg{response, make(chan struct{})}
	c.applyCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

// check task status
func (c *Coordinator) checkTaskStatus(jobType Type, msg *applyMsg) bool {
	TasksDone := true
	for _, task := range c.tasks[jobType] {
		if task.TaskStatus == Done && time.Now().After(task.DeadLine) {
			task.TaskStatus = Waiting
		}
		if task.TaskStatus == Waiting {
			msg.response.JobType = jobType
			msg.response.TaskId = task.TaskId
			msg.response.FileName = task.MapInputFile
			msg.response.NReduce = c.nReduce
			task.DeadLine = time.Now().Add(10 * time.Second)
			task.TaskStatus = Working
			return false
		}
		if task.TaskStatus != Done {
			TasksDone = false
		}
	}
	if !TasksDone {
		msg.response.JobType = WaitTask
		return false
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-c.doneCh:
		time.Sleep(3 * time.Second)
		return true
	default:
		return false
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nMap:     len(files),
		nReduce:  nReduce,
		tasks:    map[Type][]*Task{},
		applyCh:  make(chan applyMsg),
		reportCh: make(chan reportMsg),
		doneCh:   make(chan struct{}),
	}
	c.tasks[MapTask] = make([]*Task, 0, c.nMap)
	c.tasks[ReduceTask] = make([]*Task, 0, c.nReduce)
	// Your code here.
	// create map reduce task
	for _, file := range c.files {
		c.tasks[MapTask] = append(c.tasks[MapTask], &Task{
			MapInputFile: file,
			TaskId:       len(c.tasks[MapTask]),
			TaskStatus:   Waiting,
		})
	}
	for i := 0; i < c.nReduce; i++ {
		c.tasks[ReduceTask] = append(c.tasks[ReduceTask], &Task{
			MapInputFile: "",
			TaskId:       len(c.tasks[ReduceTask]),
			TaskStatus:   Waiting,
		})
	}
	go func() {
		for {
			select {
			case msg := <-c.applyCh:
				if c.checkTaskStatus(MapTask, &msg) || c.checkTaskStatus(ReduceTask, &msg) {
					msg.response.JobType = ExitTask
					c.doneCh <- struct{}{}
				}
				fmt.Printf("ApplyResponse: %#v\n", msg.response)
				msg.ok <- struct{}{}
			case msg := <-c.reportCh:
				for _, task := range c.tasks[msg.request.JobType] {
					if task.TaskId == msg.request.TaskId {
						task.TaskStatus = msg.request.TaskStatus
					}
				}
				fmt.Printf("ReportResponse: %#v\n", msg.request)
				msg.ok <- struct{}{}
			}
		}
	}()
	c.server()
	return &c
}
