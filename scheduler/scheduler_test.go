package scheduler

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type counter struct {
	value int
}

var ct *counter

func incrementCounter() {
	log.Println("counter incremented")
	ct.value++
}

func TestScheduler(t *testing.T) {

	ct = &counter{value: 0}

	s := NewScheduler()

	st1 := time.Now().Add(time.Second * 1).Format("15:04")
	st2 := time.Now().Add(time.Second * 2).Format("15:04")
	st3 := time.Now().Add(time.Second * 30).Format("15:04")

	s.AddTask("key1", []ScheduleTime{ScheduleTime(st1), ScheduleTime(st2), ScheduleTime(st3)}, incrementCounter)

	task := NewTask("key2", []ScheduleTime{ScheduleTime(st1), ScheduleTime(st2), ScheduleTime(st3)}, incrementCounter)
	s.WithTask(task)

	assert.Equal(t, 2, len(s.tasks))

	s.Start()

	task3 := NewTask("key3", []ScheduleTime{ScheduleTime(st1), ScheduleTime(st2), ScheduleTime(st3)}, incrementCounter)
	s.WithTask(task3)

	time.Sleep(5 * time.Second)

	assert.Equal(t, 3, ct.value)

	s.RemoveTask("key1")
	assert.Equal(t, 2, len(s.tasks))

	s.Stop()
}
