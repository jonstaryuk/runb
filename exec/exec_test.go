package exec_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/jonstaryuk/runb/exec"
)

type task struct {
	*exec.Task

	t      *testing.T
	events []exec.Event
}

func (t *task) Log(e exec.Event) {
	t.t.Logf("event: %#v", e)
	t.events = append(t.events, e)
}

func newTask(t *testing.T, binary string, args ...string) *task {
	task := &task{Task: exec.NewTask(binary, args...), t: t}
	task.Task.Log = task
	return task
}

func TestAdmitNormal(t *testing.T) {
	task := newTask(t, "date")

	err := task.Admit(500 * time.Millisecond)
	if assert.IsType(t, exec.Exited{}, err) {
		e := err.(exec.Exited)
		assert.InDelta(t, 5*time.Millisecond, e.Runtime, float64(5*time.Millisecond))
	}

	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Launched{}, task.events[0])
	assert.IsType(t, exec.Exited{}, task.events[1])
}

func TestAdmitAbort(t *testing.T) {
	task := newTask(t, "cat", "nonexistent-file")
	stderr := bytes.NewBuffer(nil)
	task.Stderr = stderr

	err := task.Admit(500 * time.Millisecond)
	if assert.IsType(t, exec.Exited{}, err) {
		e := err.(exec.Exited)
		assert.InDelta(t, 5*time.Millisecond, e.Runtime, float64(5*time.Millisecond))
	}

	assert.Equal(t, "cat: nonexistent-file: No such file or directory\n", stderr.String())

	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Launched{}, task.events[0])
	assert.IsType(t, exec.Exited{}, task.events[1])
}

func TestAdmitDetach(t *testing.T) {
	t.Parallel()

	task := newTask(t, "sleep", "1")

	start := time.Now()
	err := task.Admit(100 * time.Millisecond)
	assert.Nil(t, err)
	assert.WithinDuration(t, start.Add(100*time.Millisecond), time.Now(), 10*time.Millisecond)

	assert.Len(t, task.events, 1)
	assert.IsType(t, exec.Launched{}, task.events[0])

	time.Sleep(1 * time.Second)
	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Exited{}, task.events[1])
}

func TestCancel(t *testing.T) {
	task := newTask(t, "sleep", "1")

	start := time.Now()
	err := task.Admit(0)
	assert.Nil(t, err)
	assert.WithinDuration(t, start.Add(5*time.Millisecond), time.Now(), 10*time.Millisecond)

	err = task.Cancel(100 * time.Millisecond)
	assert.Nil(t, err)

	err = task.Cancel(100 * time.Millisecond)
	assert.NotNil(t, err)
}
