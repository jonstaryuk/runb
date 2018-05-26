package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// A Task holds the parameters for running a binary. It must not be modified
// while running.
type Task struct {
	Binary string            // Path to the binary
	Args   []string          // Arguments
	Env    map[string]string // Environment variables
	Stdin  io.Reader         // Defaults to an empty reader
	Stdout io.Writer         // Defaults to os.Stdout
	Stderr io.Writer         // Defaults to os.Stderr

	// Backoff will be consulted when restarting. The default is an
	// exponential backoff capped at a maximum interval of 15 seconds with
	// unlimited elapsed time.
	Backoff backoff.BackOff

	RestartOnSuccess bool // Whether to restart if it exits with status code 0

	// How long to wait after sending an interrupt, before killing the process.
	StopGracePeriod time.Duration

	EventLogger // Defaults to a new StderrEventLogger

	running *semaphore.Weighted
	cmd     *exec.Cmd
}

func NewTask(binary string, args ...string) *Task {
	t := &Task{
		Binary: binary,
		Args:   args,
	}

	if t.Stdin == nil {
		t.Stdin = bytes.NewReader([]byte{})
	}
	if t.Stdout == nil {
		t.Stdout = os.Stdout
	}
	if t.Stderr == nil {
		t.Stderr = os.Stderr
	}

	if t.EventLogger == nil {
		t.EventLogger = StderrEventLogger{Prefix: t.Binary}
	}

	if t.Backoff == nil {
		bo := backoff.NewExponentialBackOff()
		bo.MaxInterval = 15 * time.Second
		bo.MaxElapsedTime = 0
		t.Backoff = bo
	}

	t.running = semaphore.NewWeighted(1)

	return t
}

// Run runs the task, restarting if so specified. It blocks while the task is
// running. A task cannot have multiple instances running concurrently; it is
// an error to call Run while another call on the same task is still blocked.
//
// The running task can be stopped by canceling the provided context. The
// process will first receive an interrupt signal. If it does not stop after
// t.StopGracePeriod, it will be killed.
func (t *Task) Run(ctx context.Context) error {
	if !t.running.TryAcquire(1) {
		return errors.New("task is already running")
	}
	defer t.running.Release(1)

	var start time.Time
	err := backoff.Retry(func() error {
		t.cmd = t.newCmd()

		if err := t.cmd.Start(); err != nil {
			t.Log(LaunchFailed{error: err})
			return errors.Wrap(err, "start process")
		}
		start = time.Now()
		t.Log(Launched{Process: t.cmd.Process})

		stopped := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				t.cmd.Process.Signal(os.Interrupt)
				select {
				case <-stopped:
					return
				case <-time.After(t.StopGracePeriod):
					t.cmd.Process.Kill()
				}
			case <-stopped:
				return
			}
		}()
		err := t.cmd.Wait()
		close(stopped)
		t.Log(Exited{Err: err, Runtime: time.Now().Sub(start)})

		select {
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err())
		default:
			if err != nil {
				return err
			}
			if t.RestartOnSuccess {
				return errors.New("restarting on success")
			}
			return nil
		}
	}, t.Backoff)

	if permerr, ok := err.(*backoff.PermanentError); ok {
		return permerr.Err
	}
	return err
}

func (t Task) newCmd() *exec.Cmd {
	cmd := exec.Command(t.Binary, t.Args...)

	cmd.Env = []string{}
	for k, v := range t.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	cmd.Stdin = t.Stdin
	cmd.Stdout = t.Stdout
	cmd.Stderr = t.Stderr

	return cmd
}

type EventLogger interface {
	Log(e Event) // Any particular Task will only call Log sequentially
}

type Event interface {
	String() string
}

type Launched struct {
	Process *os.Process
}

func (Launched) String() string { return "launched" }

type LaunchFailed struct {
	error
}

func (l LaunchFailed) String() string { return l.Error() }

type Exited struct {
	Err     error
	Runtime time.Duration
}

func (e Exited) String() string {
	msg := fmt.Sprintf("process ended after %v", e.Runtime.Round(time.Millisecond))
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

type StderrEventLogger struct {
	Prefix string
}

func (s StderrEventLogger) Log(e Event) {
	log.Printf("exec %s: %s", s.Prefix, e.String())
}

type NoopLogger struct{}

func (NoopLogger) Log(e Event) {}
