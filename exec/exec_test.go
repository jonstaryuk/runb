package exec_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/stretchr/testify/assert"

	"github.com/jonstaryuk/runb/exec"
)

const ms = time.Millisecond

type taskWrapper struct {
	*exec.Task

	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	events []exec.Event
}

func (t *taskWrapper) Log(e exec.Event) {
	t.t.Logf("event: %s", e)
	t.events = append(t.events, e)
}

func newTask(t *testing.T, timeout time.Duration, binary string, args ...string) *taskWrapper {
	task := &taskWrapper{Task: exec.NewTask(binary, args...), t: t}
	task.Task.EventLogger = task
	task.ctx, task.cancel = context.WithTimeout(context.Background(), timeout)
	return task
}

func in(d time.Duration, f func()) {
	go func() {
		time.Sleep(d)
		f()
	}()
}

func TestAdmitNormal(t *testing.T) {
	task := newTask(t, 100*ms, "date")

	err := task.Run(task.ctx, 0)
	assert.NoError(t, err)

	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Launched{}, task.events[0])
	if assert.IsType(t, exec.Exited{}, task.events[1]) {
		assert.InDelta(t, 5*ms, task.events[1].(exec.Exited).Runtime, float64(5*ms))
	}
}

func TestAdmitFailure(t *testing.T) {
	t.Parallel()

	task := newTask(t, 1000*ms, "cat", "nonexistent-file")
	stderr := bytes.NewBuffer(nil)
	task.Stderr = stderr
	task.Backoff = backoff.WithMaxRetries(task.Backoff, 1)

	err := task.Run(task.ctx, 0)
	assert.NotNil(t, err)

	assert.Equal(t, strings.Repeat("cat: nonexistent-file: No such file or directory\n", 2), stderr.String())

	assert.Len(t, task.events, 4)
	assert.IsType(t, exec.Launched{}, task.events[0])
	assert.IsType(t, exec.Exited{}, task.events[1])
	assert.InDelta(t, 5*ms, task.events[1].(exec.Exited).Runtime, float64(5*ms))
	assert.IsType(t, exec.Launched{}, task.events[2])
	assert.IsType(t, exec.Exited{}, task.events[3])
}

func TestAdmitDetach(t *testing.T) {
	t.Parallel()

	task := newTask(t, 2000*ms, "sleep", "1")

	start := time.Now()
	err := task.Run(task.ctx, 0)
	assert.NoError(t, err)
	assert.WithinDuration(t, start.Add(1000*ms), time.Now(), 50*ms)

	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Launched{}, task.events[0])
	assert.IsType(t, exec.Exited{}, task.events[1])
}

func TestCancel(t *testing.T) {
	t.Parallel()

	task := newTask(t, 2000*ms, "sleep", "1")

	start := time.Now()
	in(500*ms, task.cancel)
	err := task.Run(task.ctx, 100*ms)
	assert.Equal(t, context.Canceled, err)
	assert.WithinDuration(t, start.Add(500*ms), time.Now(), 20*ms)

	assert.Len(t, task.events, 2)
	assert.IsType(t, exec.Launched{}, task.events[0])
	if assert.IsType(t, exec.Exited{}, task.events[1]) {
		assert.EqualError(t, task.events[1].(exec.Exited).Err, "signal: interrupt")
	}
}

func TestEnv(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	task := newTask(t, 100*ms, "env")
	task.Stdout = stdout
	task.Stderr = stderr
	err := task.Run(task.ctx, 0)
	assert.NoError(t, err)
	assert.Empty(t, stdout.String())
	assert.Empty(t, stderr.String())

	task.Env = map[string]string{"foo": "bar"}
	stdout.Reset()
	stderr.Reset()
	err = task.Run(task.ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, "foo=bar\n", stdout.String())
	assert.Empty(t, stderr.String())
}

func TestAlreadyRunning(t *testing.T) {
	t.Parallel()

	task := newTask(t, 1500*ms, "sleep", "1")

	go func() {
		err := task.Run(task.ctx, 0)
		assert.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)
	err := task.Run(task.ctx, 0)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "already running")
	}
}
