package scheduler

import (
	"sync"
	"time"
)

type Task struct {
	Key          string
	ScheduleTime []ScheduleTime
	LastExec     map[ScheduleTime]time.Time
	Execution    func()
	mu           sync.Mutex
}

type ScheduleTime string

func NewTask(key string, scheduleTime []ScheduleTime, execution func()) *Task {
	return &Task{
		Key:          key,
		ScheduleTime: scheduleTime,
		LastExec:     make(map[ScheduleTime]time.Time),
		Execution:    execution,
	}
}
