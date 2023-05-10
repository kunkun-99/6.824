package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	taskId            int
	nReduce           int
	filenames         []string
	stagePhase        Phase      // 整个任务的阶段
	taskInfoGroup     []TaskInfo // 存有所有的任务
	taskChannelReduce chan *Task
	taskChannelMap    chan *Task
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	ExitPhase
)

type TaskInfo struct {
	taskState   TaskState
	task        *Task
	currentTime time.Time
}

type TaskState int

const (
	Working TaskState = iota
	Wating
	Finished
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CheckDone() bool {
	var (
		mapDoneNum      = 0
		mapUndoneNum    = 0
		reduceDoneNum   = 0
		reduceUndoneNum = 0
	)
	for _, taskInfo := range c.taskInfoGroup {
		// 说明还没定义该任务，直接跳过
		if taskInfo.task == nil {
			break
		}
		if taskInfo.task.TaskType == MapTask {
			if taskInfo.taskState == Finished {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else if taskInfo.task.TaskType == ReduceTask {
			if taskInfo.taskState == Finished {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	if mapDoneNum > 0 && mapUndoneNum == 0 && reduceDoneNum == 0 && reduceUndoneNum == 0 {
		return true
	}
	if mapDoneNum > 0 && mapUndoneNum == 0 && reduceDoneNum > 0 && reduceUndoneNum == 0 {
		return true
	}
	return false
}

func (c *Coordinator) ToNextPhase() {
	switch c.stagePhase {
	case MapPhase:
		{
			c.MakeReduceTask()
			c.stagePhase = ReducePhase
		}
	case ReducePhase:
		{
			c.stagePhase = ExitPhase
		}
	}
}

func (c *Coordinator) PullTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.stagePhase {
	case MapPhase:
		{
			if len(c.taskChannelMap) > 0 {
				*reply = *<-c.taskChannelMap
				if c.taskInfoGroup[reply.TaskId].taskState == Working {
					fmt.Printf("TaskId %d is already working! \n", reply.TaskId)
				}
				c.taskInfoGroup[reply.TaskId].taskState = Working
				c.taskInfoGroup[reply.TaskId].currentTime = time.Now()
			} else {
				fmt.Println("Pull Waiting Task")
				reply.TaskType = WaitingTask
				if c.CheckDone() {
					c.ToNextPhase()
				}
			}
		}
	case ReducePhase:
		{
			if len(c.taskChannelReduce) > 0 {
				*reply = *<-c.taskChannelReduce
				if c.taskInfoGroup[reply.TaskId].taskState == Working {
					fmt.Printf("TaskId %d is already working! \n", reply.TaskId)
				}
				c.taskInfoGroup[reply.TaskId].taskState = Working
				c.taskInfoGroup[reply.TaskId].currentTime = time.Now()
			} else {
				reply.TaskType = WaitingTask
				if c.CheckDone() {
					c.ToNextPhase()
				}

			}
		}
	case ExitPhase:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	taskId := args.TaskId
	if taskId != c.taskInfoGroup[taskId].task.TaskId {
		fmt.Println("in taskDone:Task's taskId not equal to c.taskInfoGroup's taskId")
	}
	if c.taskInfoGroup[taskId].taskState != Working {
		fmt.Println("in taskDone:the task's state is waiting or already finished")
	}
	c.taskInfoGroup[taskId].taskState = Finished
	return nil
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
	mu.Lock()
	defer mu.Unlock()
	ret := false

	// Your code here.
	if c.stagePhase == ExitPhase {
		ret = true
	}

	return ret
}

func (c *Coordinator) MakeMapTask(files []string) {
	for _, file := range files {
		task := Task{
			TaskType:  MapTask,
			TaskId:    c.GenerateTaskId(),
			ReduceNum: c.nReduce,
			Filenames: []string{file},
		}
		c.taskChannelMap <- &task
		taskInfo := TaskInfo{
			taskState: Wating,
			task:      &task,
		}
		c.taskInfoGroup[task.TaskId] = taskInfo
	}
}

func (c *Coordinator) MakeReduceTask() {
	for i := 0; i < c.nReduce; i++ {
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    c.GenerateTaskId(),
			ReduceNum: c.nReduce,
			Filenames: SelectReduceFiles(i),
		}
		c.taskChannelReduce <- &task
		taskInfo := TaskInfo{
			taskState: Wating,
			task:      &task,
		}
		c.taskInfoGroup[task.TaskId] = taskInfo
	}
}

func SelectReduceFiles(i int) []string {
	path, _ := os.Getwd()
	rd, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println("read dir fail:", err)
	}
	var ret []string
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-") && strings.HasSuffix(fi.Name(), "-"+strconv.Itoa(i)) {
			ret = append(ret, fi.Name())
		}
	}
	return ret
}

func (c *Coordinator) GenerateTaskId() int {
	taskId := c.taskId
	c.taskId++
	return taskId
}

func (c *Coordinator) CrashDectector() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()
		for i, taskInfo := range c.taskInfoGroup {
			if taskInfo.task == nil {
				continue
			}
			if taskInfo.taskState == Working && time.Since(taskInfo.currentTime) > 9*time.Second {
				switch taskInfo.task.TaskType {
				case MapTask:
					{
						c.taskChannelMap <- taskInfo.task
						c.taskInfoGroup[i].taskState = Wating

					}
				case ReduceTask:
					{
						c.taskChannelReduce <- taskInfo.task
						c.taskInfoGroup[i].taskState = Wating
					}
				}

			}
		}
		mu.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:           nReduce,
		filenames:         files,
		stagePhase:        MapPhase,
		taskChannelReduce: make(chan *Task, nReduce),
		taskChannelMap:    make(chan *Task, len(files)),
		taskInfoGroup:     make([]TaskInfo, len(files)+nReduce),
	}
	c.MakeMapTask(c.filenames)
	c.server()
	go c.CrashDectector()
	return &c
}
