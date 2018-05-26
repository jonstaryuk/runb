package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
)

// A Task holds the parameters for running a binary.
type Task struct {
	Binary string            // Path to the binary
	Args   []string          // Arguments
	Env    map[string]string // Environment variables
	Stdin  io.Reader         // Defaults to empty
	Stdout io.Writer         // Defaults to this program's stdout
	Stderr io.Writer         // Defaults to this program's stderr

	// Backoff will be consulted when restarting. The default is an
	// exponential backoff capped at a maximum interval of 15 seconds with
	// unlimited elapsed time.
	Backoff backoff.BackOff

	RestartOnSuccess bool // Whether to restart if it exits with status code 0

	EventLogger // Defaults to a new StderrEventLogger

	running sync.Mutex
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

	return t
}

func timeout(f func(), d time.Duration) (ok bool) {
	done := make(chan struct{})

	go func() {
		f()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// Run runs the task, restarting if so specified. It must not be called if the
// task is currently running.
//
// The running task can be stopped by canceling the provided context. The
// process will first receive an interrupt signal. If it does not stop after
// gracePeriod, it will be killed.
func (t *Task) Run(ctx context.Context, gracePeriod time.Duration) error {
	if !timeout(func() { t.running.Lock() }, 100*time.Millisecond) {
		return errors.New("Run() must not be called if the task is already running")
	}
	defer t.running.Unlock()

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
				case <-time.After(gracePeriod):
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
