package mr

import "time"

type TaskPhase int

const (
	Unknown     TaskPhase = 0
	MapPhase    TaskPhase = 1
	ReducePhase TaskPhase = 2
)

type TaskState int

const (
	TaskReady TaskState = iota
	TaskRunning
	TaskDone
	TaskError
)

type TaskStatus struct {
	TaskState TaskState
	WorkerId  int
	Deadline  time.Time
	Task      *Task
}

type SplitType int

const (
	FILENAME SplitType = 0
)

type InputSplit struct {
	Split     *KeyValue
	ValueType SplitType
}

type Task struct {
	InputSplit *InputSplit
	Phase      TaskPhase
	NMap       int
	NReduce    int
	TaskIndex  int
}
