package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Timeout = 10
)

type Coordinator struct {
	// Your definitions here.
	mutex   sync.Mutex
	files   []string
	nMap    int
	nReduce int
	phase   Phase
	taskMap map[int]Task
	taskCh  chan Task
}

// utils
func tmpMapOutFile(worker string, mi int, ri int) string {
	return fmt.Sprintf("tmp-map-%s-%d-%d", worker, mi, ri)
}

func finalMapOutFile(mi int, ri int) string {
	return fmt.Sprintf("mr-%d-%d", mi, ri)
}

func tmpReduceOutFile(worker string, ri int) string {
	return fmt.Sprintf("tmp-reduce-%s-%d", worker, ri)
}

func finalReduceOutFile(ri int) string {
	return fmt.Sprintf("mr-out-%d", ri)
}

func (c *Coordinator) initMapPhase() {
	for i, file := range c.files {
		task := Task{
			TaskId:        i,
			TaskType:      MapTask,
			TaskInputFile: file,
			MapNum:        c.nMap,
			ReduceNum:     c.nReduce,
		}
		log.Printf("initMapPhase taskid %d nReduce %d\n", task.TaskId, task.ReduceNum)
		c.taskMap[i] = task
		c.taskCh <- task
	}
	log.Printf("initMapPhase sucess\n")
}

func (c *Coordinator) initReducePhase() {
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			TaskId:    i,
			TaskType:  ReduceTask,
			MapNum:    c.nMap,
			ReduceNum: c.nReduce,
		}
		c.taskMap[i] = task
		c.taskCh <- task
	}
	log.Printf("initReducePhase sucess\n")
}

func (c *Coordinator) initNextPhase() {
	switch c.phase {
	case Map:
		c.phase = Reduce
		c.initReducePhase()
	case Reduce:
		close(c.taskCh)
		c.phase = Done
		log.Printf("All tasks Done\n")
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(request *ApplyRequest, response *Task) error {
	if request.CompletedTask != nil {
		c.verifyAndCommit(request)
	}
	task, ok := <-c.taskCh
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.phase == Done {
		task.TaskType = CompletedTask
	} else if !ok {
		task.TaskType = WaitTask
		log.Printf("worker %s is waitting\n", task.WorkerId)
	} else {
		task.WorkerId = request.WorkerId
		task.StartTime = time.Now()
		c.taskMap[task.TaskId] = task
		log.Printf("Assign %d task %d to worker %s\n", task.TaskType, task.TaskId, task.WorkerId)
	}
	*response = task
	return nil
}

func (c *Coordinator) verifyAndCommit(request *ApplyRequest) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := request.CompletedTask
	_, exist := c.taskMap[task.TaskId]
	log.Printf("get verify taskid %d\n", task.TaskId)
	if exist && task.WorkerId == request.WorkerId {
		switch task.TaskType {
		case MapTask:
			for ri := 0; ri < c.nReduce; ri++ {
				err := os.Rename(
					tmpMapOutFile(task.WorkerId, task.TaskId, ri),
					finalMapOutFile(task.TaskId, ri))
				if err != nil {
					log.Printf(
						"Failed to get map output file `%s` as final: %e",
						tmpMapOutFile(task.WorkerId, task.TaskId, ri), err)
				}
			}
		case ReduceTask:
			err := os.Rename(
				tmpReduceOutFile(task.WorkerId, task.TaskId),
				finalReduceOutFile(task.TaskId))
			if err != nil {
				log.Printf(
					"Failed to get reduce output file `%s` as final: %e",
					tmpReduceOutFile(task.WorkerId, task.TaskId), err)
			}
		}
		log.Printf("verify taskid %d success\n", task.TaskId)
		delete(c.taskMap, task.TaskId)
		if len(c.taskMap) == 0 {
			c.initNextPhase()
		}
	}
}

func (c *Coordinator) checkTaskStatus() {
	for {
		time.Sleep(1000 * time.Millisecond)
		c.mutex.Lock()
		if c.phase == Done {
			c.mutex.Unlock()
			break
		}
		for _, task := range c.taskMap {
			if task.WorkerId != "" && time.Now().After(task.StartTime.Add(Timeout*time.Second)) {
				log.Printf("TimeOut crash: %d task %d  workerId %s.", task.TaskType, task.TaskId, task.WorkerId)
				task.WorkerId = ""
				c.taskCh <- task
			}
		}
		c.mutex.Unlock()
	}
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.phase == Done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// input filenames
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	n := int(math.Max(float64(len(files)), float64(nReduce))) // channel capacity
	c := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
		phase:   Map,
		taskMap: make(map[int]Task),
		taskCh:  make(chan Task, n),
	}
	log.Printf("Coordinator Start!\n")
	c.initMapPhase()
	c.server()
	go c.checkTaskStatus()
	return &c
}
