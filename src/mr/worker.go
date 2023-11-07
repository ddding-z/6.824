package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// Steal from main/mrsequential.go
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	var completedTask *Task
	for {
		request := ApplyRequest{
			WorkerId:      strconv.Itoa(os.Getpid()),
			CompletedTask: completedTask,
		}
		response := Task{}
		call(
			"Coordinator.AssignTask",
			&request,
			&response,
		)
		//log.Printf("response taskid %d workerId %s\n", response.TaskId, response.WorkerId)
		switch response.TaskType {
		case MapTask:
			handleMapTask(mapf, &response)
			completedTask = &response
			//log.Printf("Worker: Finished Map Task %d", response.TaskId)
		case ReduceTask:
			handleReduceTask(reducef, &response)
			completedTask = &response
			//log.Printf("Worker: Finished Reduce Task %d", response.TaskId)
		case WaitTask:
			//log.Printf("Worker: wait one second %d", response.TaskId)
			time.Sleep(1000 * time.Millisecond)
			completedTask = nil
		case CompletedTask:
			//log.Printf("Finished All Tasks")
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func handleMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// steal from main/mrsequential.go
	file, err := os.Open(task.TaskInputFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open %v", task.TaskInputFile))
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		panic(fmt.Sprintf("Failed to read %v", task.TaskInputFile))
	}
	file.Close()

	kva := mapf(task.TaskInputFile, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hashedv := ihash(kv.Key) % task.ReduceNum
		hashedKva[hashedv] = append(hashedKva[hashedv], kv)
	}

	for i := 0; i < task.ReduceNum; i++ {
		ofile, _ := os.Create(tmpMapOutFile(task.WorkerId, task.TaskId, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
}

func handleReduceTask(reducef func(string, []string) string, task *Task) {
	var lines []string
	for mi := 0; mi < task.MapNum; mi++ {
		inputFile := finalMapOutFile(mi, task.TaskId)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}
	sort.Sort(ByKey(kva))

	// steal from main/mrsequential.go
	ofile, _ := os.Create(tmpReduceOutFile(task.WorkerId, task.TaskId))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
