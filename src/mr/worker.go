package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

func DoMapTask(mapf func(string, string) []KeyValue, reply *Task) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.Filenames[0])
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filenames[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filenames[0])
	}
	file.Close()
	kva := mapf(reply.Filenames[0], string(content))
	intermediate = append(intermediate, kva...)

	rn := reply.ReduceNum
	rnHash := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		rnHash[ihash(kv.Key)%rn] = append(rnHash[ihash(kv.Key)%rn], kv)
	}
	// 创建临时文件
	for i := 0; i < rn; i++ {
		tempFilename := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		fp, err := os.Create(tempFilename)
		if err != nil {
			fmt.Println("文件创建失败。")
			return
		}
		enc := json.NewEncoder(fp)
		for _, kv := range rnHash[i] {
			enc.Encode(&kv)
		}
		fp.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, reply *Task) {
	kva := []KeyValue{}
	for _, filename := range reply.Filenames {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	oname := "mr-out-" + string(reply.Filenames[0][len(reply.Filenames[0])-1])
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	flag := true
	for flag {
		task := CallTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				CallDone(&task)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				CallDone(&task)
			}
		case WaitingTask:
			{
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				flag = false
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

func CallTask() Task {

	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.PullTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("call task ok\n")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func CallDone(task *Task) {

	// declare a reply structure.
	reply := Task{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.TaskDone", task, &reply)
	if ok {
		fmt.Printf("CallDone is ok\n")
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
