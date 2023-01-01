package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	NMap    int      // the number of map tasks
	NReduce int      // the number of reduce tasks
	Files   []string //the files to use
	TaskNum int

	WorkPhase      int // the phase of work
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	TaskManager    TaskStateSet // the manager of the states of tasks

	mu sync.Mutex
}

type TaskStateSet struct {
	TaskState map[int]*TaskStateInfo
}

type TaskStateInfo struct {
	State     int
	StartTime time.Time // for crash handler
	TaskPtr   *Task
}

// Your code here -- RPC handlers for the worker to call.

// Assign tasks to wokers
func (c *Coordinator) AssignTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.WorkPhase {
	case MapPhase:
		if len(c.MapTaskChan) > 0 {
			// map tasks haven't been completed
			task := *<-c.MapTaskChan
			*reply = WorkerReply{
				TaskType: task.TaskType,
				NReduce:  c.NReduce,
				NMap:     c.NMap,
				TaskId:   task.TaskId,
				Filename: task.Filename,
			}
			// change the state of task
			taskState := c.TaskManager.TaskState[task.TaskId]
			taskState.State = WorkingState
			taskState.StartTime = time.Now()
		} else {
			// All map tasks have been completed or in-progress
			reply.TaskType = WaitingTask
			if c.TaskManager.checkTaskDone() {
				c.switchToNextPhase()
			}
		}
	case ReducePhase:
		if len(c.ReduceTaskChan) > 0 {
			// reduce tasks haven't been completed
			task := *<-c.ReduceTaskChan
			*reply = WorkerReply{
				TaskType: task.TaskType,
				NReduce:  c.NReduce,
				NMap:     c.NMap,
				TaskId:   task.TaskId,
			}
			// change the state of task
			taskState := c.TaskManager.TaskState[task.TaskId]
			taskState.State = WorkingState
			taskState.StartTime = time.Now()
		} else {
			// All reduce tasks have been completed or in-progress
			reply.TaskType = WaitingTask
			if c.TaskManager.checkTaskDone() {
				c.switchToNextPhase()
			}
		}
	case ExitPhase:
		reply.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) switchToNextPhase() {
	if c.WorkPhase == MapPhase {
		c.loadReduceTask()
		c.WorkPhase = ReducePhase
	} else if c.WorkPhase == ReducePhase {
		c.WorkPhase = ExitPhase
	}
}

// check if all the tasks of current state are done
func (t *TaskStateSet) checkTaskDone() bool {
	var (
		mapDone      = 0
		mapUndone    = 0
		reduceDone   = 0
		reduceUndone = 0
	)

	for _, v := range t.TaskState {
		if v.TaskPtr.TaskType == MapTask {
			if v.State == DoneState {
				mapDone++
			} else {
				mapUndone++
			}
		} else if v.TaskPtr.TaskType == ReduceTask {
			if v.State == DoneState {
				reduceDone++
			} else {
				reduceUndone++
			}
		}
	}

	if (mapDone > 0 && mapUndone == 0) && (reduceDone == 0 && reduceUndone == 0) {
		// in map phase and all map tasks are done
		return true
	} else {
		// in reduce phase and all reduce tasks are done
		if reduceDone > 0 && reduceUndone == 0 {
			return true
		}
	}
	return false
}

// New task finished
func (c *Coordinator) TaskFinished(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		taskState := c.TaskManager.TaskState[args.TaskId]
		taskState.State = DoneState
	case ReduceTask:
		taskState := c.TaskManager.TaskState[args.TaskId]
		taskState.State = DoneState
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.WorkPhase == ExitPhase

	// return ret
}

func (c *Coordinator) loadMapTask() {
	for _, v := range c.Files {
		taskId := c.TaskNum
		c.TaskNum++
		task := Task{
			TaskType: MapTask,
			TaskId:   taskId,
			Filename: v,
		}

		// initialize the map task
		taskStateInfo := TaskStateInfo{
			State:   WaitingState,
			TaskPtr: &task,
		}
		c.TaskManager.TaskState[taskId] = &taskStateInfo

		c.MapTaskChan <- &task
	}
}

func (c *Coordinator) loadReduceTask() {
	for i := 0; i < c.NReduce; i++ {
		taskId := c.TaskNum
		c.TaskNum++
		task := Task{
			TaskType: ReduceTask,
			TaskId:   taskId,
		}

		// initialize the reduce task
		taskStateInfo := TaskStateInfo{
			State:   WaitingState,
			TaskPtr: &task,
		}
		c.TaskManager.TaskState[taskId] = &taskStateInfo

		c.ReduceTaskChan <- &task
	}
}

func (c *Coordinator) CrashHandler() {
	for {
		c.mu.Lock()

		if c.WorkPhase == ExitPhase {
			c.mu.Unlock()
			break
		}

		for _, v := range c.TaskManager.TaskState {
			// the task crash
			if v.State == WorkingState && time.Since(v.StartTime) >= 10*time.Second {
				v.State = WaitingState
				switch v.TaskPtr.TaskType {
				case MapTask:
					c.MapTaskChan <- v.TaskPtr
				case ReduceTask:
					c.ReduceTaskChan <- v.TaskPtr
				}
			}
		}

		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:           len(files),
		NReduce:        nReduce,
		Files:          files,
		TaskNum:        0,
		WorkPhase:      MapPhase,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		TaskManager:    TaskStateSet{TaskState: make(map[int]*TaskStateInfo, len(files)+nReduce)},
	}

	// Your code here.
	c.loadMapTask()
	c.server()
	go c.CrashHandler()

	return &c
}
