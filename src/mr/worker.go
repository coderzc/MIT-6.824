package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	MapFileDir     = "."
	InitReduceSize = 10
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func parseInputSplit(split *InputSplit) (*KeyValue, error) {
	switch split.ValueType {
	case FILENAME:
		filename := split.Split.Value
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("cannot open %v", filename)
		}
		defer file.Close()
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("cannot read %v", filename)
		}

		inputSplit := KeyValue{
			Key:   filename,
			Value: string(content),
		}
		return &inputSplit, nil
	default:
		return nil, fmt.Errorf("cannot identify valueType: %d", split.ValueType)
	}
}

func reduceName(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("%s/mr-%d-%d.json", MapFileDir, mapIndex, reduceIndex)
}

func outputName(reduceIndex int) string {
	return fmt.Sprintf("%s/mr-out-%d.txt", MapFileDir, reduceIndex)
}

type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// Start up map worker
	worker := worker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.workerId = os.Getpid()
	worker.register()
	worker.run()
}

func (w worker) register() {
	args := RegisterArgs{
		WorkerId: w.workerId,
	}
	reply := RegisterReply{}
	call("Coordinator.RegWorker", &args, &reply)
}

func (w *worker) run() {
	for true {
		taskArgs := TaskArgs{WorkerId: w.workerId}
		taskReply := TaskReply{}
		call("Coordinator.PullTask", &taskArgs, &taskReply)

		if taskReply.Done {
			log.Printf("no more tasks, worker[%v] quit", w.workerId)
			return
		}

		task := taskReply.Task
		log.Printf("pull a task on worker[%v], task:%+v", w.workerId, task)
		if task == nil || !task.Alive {
			// ignore it
			continue
		}

		var err error
		switch task.Phase {
		case MapPhase:
			err = w.doMapTask(task)
		case ReducePhase:
			err = w.doReduceTask(task)
		default:
			continue
		}

		if err != nil {
			log.Printf("exec task[%v] failed: %+v", task.TaskIndex, err)
			w.reportTask(task, false)
		} else {
			w.reportTask(task, true)
		}
	}
}

func (w worker) reportTask(task *Task, succeeded bool) {
	reportArgs := ReportTaskArgs{
		Succeeded: succeeded,
		WorkerId:  w.workerId,
		TaskIndex: task.TaskIndex,
		Phase:     task.Phase,
	}
	reportReply := ReportTaskReply{}

	call("Coordinator.ReportTask", &reportArgs, &reportReply)
}

func (w worker) doMapTask(task *Task) error {
	inputSplit, err := parseInputSplit(task.InputSplit)
	if err != nil {
		log.Println("failed to load input split")
		return err
	}

	if inputSplit != nil {
		mapKVs := w.mapf(inputSplit.Key, inputSplit.Value)
		reduces := make([][]KeyValue, task.NReduce)
		for _, kv := range mapKVs {
			index := ihash(kv.Key) % task.NReduce
			reduces[index] = append(reduces[index], kv)
		}

		for index, kvs := range reduces {
			file, err := os.Create(reduceName(task.TaskIndex, index))
			if err != nil {
				return err
			}

			encoder := json.NewEncoder(file)

			for _, kv := range kvs {
				if err = encoder.Encode(&kv); err != nil {
					return err
				}
			}

			if err := file.Close(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w worker) doReduceTask(task *Task) error {
	reduces := make([]KeyValue, 0, InitReduceSize)

	// shuffle
	for mapIndex := 0; mapIndex < task.NMap; mapIndex++ {
		file, err := os.Open(reduceName(mapIndex, task.TaskIndex))
		if err != nil {
			return err
		}

		decoder := json.NewDecoder(file)

		for true {
			kv := KeyValue{}
			err = decoder.Decode(&kv)
			if err != nil {
				file.Close()
				break
			}

			reduces = append(reduces, kv)
		}
	}

	sort.Sort(ByKey(reduces))

	// reduce
	ofile, err := os.Create(outputName(task.TaskIndex))
	if err != nil {
		return err
	}
	values := make([]string, 0, InitReduceSize)
	key := ""
	for i, kv := range reduces {
		if kv.Key != key && i != 0 {
			result := w.reducef(key, values)
			if err := w.output(ofile, key, result); err != nil {
				return err
			}
			values = values[:0]
		}
		key = kv.Key
		values = append(values, kv.Value)
		if i == len(reduces)-1 {
			result := w.reducef(key, values)
			if err := w.output(ofile, key, result); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *worker) output(ofile io.Writer, key string, result string) error {
	_, err := fmt.Fprintf(ofile, "%v %v\n", key, result)
	if err != nil {
		return err
	}

	return nil
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
