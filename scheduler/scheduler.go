package scheduler

import (
	"sync"
	"time"
)

type Scheduler struct {
	tasks     map[string]*Task
	stopChan  chan struct{}
	isRunning bool
	mu        sync.Mutex
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		tasks:     make(map[string]*Task),
		stopChan:  make(chan struct{}),
		isRunning: false,
	}
}

func (s *Scheduler) AddTask(key string, schedule []ScheduleTime, execution func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := &Task{
		Key:          key,
		ScheduleTime: schedule,
		LastExec:     make(map[ScheduleTime]time.Time),
		Execution:    execution,
	}

	s.tasks[key] = task
}

func (s *Scheduler) WithTask(task *Task) *Scheduler {
	s.AddTask(task.Key, task.ScheduleTime, task.Execution)
	return s
}

func (s *Scheduler) RemoveTask(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, key)
}

func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		go s.scheduleLoop()
		s.isRunning = true
	}
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		close(s.stopChan)
		s.isRunning = false
	}
}

func (s *Scheduler) scheduleLoop() {
	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.checkAndExecute()
		case <-s.stopChan:
			return
		}
	}
}

func (s *Scheduler) checkAndExecute() {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentTime := time.Now().Format("15:04")
	currentDate := time.Now().Format("2006-01-02")

	for _, task := range s.tasks {
		for _, value := range task.ScheduleTime {
			if value == ScheduleTime(currentTime) {
				task.mu.Lock()
				lastExecuted, ok := task.LastExec[value]
				if !ok || lastExecuted.Format("2006-01-02") != currentDate {
					task.LastExec[value] = time.Now()
					go task.Execution()
				}
				task.mu.Unlock()
			}
		}
	}
}
