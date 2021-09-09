package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskTimeout      = 8 * time.Second
	ScheduleInterval = time.Millisecond * 300
)

type Coordinator struct {
	// Your definitions here.
	workerIds   []int
	nMap        int
	nReduce     int
	phase       TaskPhase
	taskCh      chan Task
	taskStatuss []*TaskStatus
	done        bool
	lock        sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) PullTask(taskArgs *TaskArgs, taskReply *TaskReply) error {
	if c.Done() {
		taskReply.Done = true
		return nil
	}

	if task, ok := <-c.taskCh; ok {
		taskReply.Task = &task

		if c.taskStatuss[task.TaskIndex].TaskState == TaskReady {
			c.lock.Lock()
			defer c.lock.Unlock()

			c.taskStatuss[task.TaskIndex].WorkerId = taskArgs.WorkerId
			c.taskStatuss[task.TaskIndex].Deadline = time.Now().Add(TaskTimeout)
			c.taskStatuss[task.TaskIndex].TaskState = TaskRunning
		}
	}

	return nil
}

func (c *Coordinator) ReportTask(reportArgs *ReportTaskArgs, reportReply *ReportTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	taskStat := c.taskStatuss[reportArgs.TaskIndex]

	if reportArgs.Phase != c.phase || reportArgs.WorkerId != taskStat.WorkerId {
		// ignore it
		return nil
	}

	if reportArgs.Succeeded {
		taskStat.TaskState = TaskDone
	} else {
		taskStat.TaskState = TaskError
	}

	log.Printf("report a task, reportReply:%+v, taskStat:%+v", reportArgs, taskStat)

	go c.schedule()
	return nil
}

func (c *Coordinator) RegWorker(regArgs *RegisterArgs, regReply *RegisterReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.workerIds = append(c.workerIds, regArgs.WorkerId)

	log.Printf("a worker register to Coordinator, workerId:%+v, workerIds:%+v", regArgs.WorkerId, c.workerIds)
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
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	return c.done
}

func (c *Coordinator) schedule() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.done {
		return
	}

	allDone := true
	for _, taskStatus := range c.taskStatuss {
		switch taskStatus.TaskState {
		case TaskRunning:
			if time.Now().After(taskStatus.Deadline) {
				taskStatus.TaskState = TaskReady
				c.taskCh <- *taskStatus.Task
			}
			allDone = false
		case TaskDone:
		case TaskError:
			taskStatus.TaskState = TaskReady
			c.taskCh <- *taskStatus.Task
			allDone = false
		case TaskReady:
			allDone = false
		}
	}

	if allDone {
		c.nextPhase()
	}
}

func (c *Coordinator) nextPhase() {
	if c.phase == ReducePhase {
		log.Printf("reduce phase finish")
		c.done = true
		close(c.taskCh)
	} else if c.phase == MapPhase {
		log.Printf("map phase finish")
		c.initReducePhase()
	}
}

func (c *Coordinator) initMapPhase(files []string) {
	log.Printf("init map phase...")

	c.phase = MapPhase
	c.nMap = len(files)
	c.taskStatuss = make([]*TaskStatus, c.nMap)

	for i, file := range files {
		task := Task{
			Phase: MapPhase,
			InputSplit: &InputSplit{
				ValueType: FILENAME,
				Split: &KeyValue{
					Key:   strconv.Itoa(i),
					Value: file,
				},
			},
			TaskIndex: i,
			NMap:      c.nMap,
			NReduce:   c.nReduce,
		}

		c.taskStatuss[i] = &TaskStatus{
			TaskState: TaskReady,
			Task:      &task,
		}
		c.taskCh <- task
	}
}

func (c *Coordinator) initReducePhase() {
	log.Printf("init reduce phase...")

	c.phase = ReducePhase
	c.taskStatuss = make([]*TaskStatus, c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		task := Task{
			Phase:     ReducePhase,
			TaskIndex: i,
			NMap:      c.nMap,
			NReduce:   c.nReduce,
		}
		c.taskStatuss[i] = &TaskStatus{
			TaskState: TaskReady,
			Task:      &task,
		}
		c.taskCh <- task
	}
}

func (c *Coordinator) startSchedule() {
	ticker := time.NewTicker(ScheduleInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.schedule()
		if c.Done() {
			break
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	if nReduce > len(files) {
		c.taskCh = make(chan Task, nReduce)
	} else {
		c.taskCh = make(chan Task, len(files))
	}

	c.initMapPhase(files)

	go c.startSchedule()

	// Your code here.
	c.server()
	log.Println("Coordinator start up succeed")
	return &c
}
