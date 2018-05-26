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

	EventLogger // Defaults to a NoopLogger
}

// NewTask creates a new Task with default values.
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
		t.EventLogger = NoopLogger{}
	}

	if t.Backoff == nil {
		bo := backoff.NewExponentialBackOff()
		bo.MaxInterval = 15 * time.Second
		bo.MaxElapsedTime = 0
		t.Backoff = bo
	}

	return t
}

// Run runs the Task, restarting if so specified. It blocks while the task is
// running. It is safe to start multiple concurrent runs.
//
// The running task can be stopped by canceling the provided context. If the
// process is running, it will first receive an interrupt signal; if it does
// not stop after t.StopGracePeriod, it will be killed.
func (t *Task) Run(ctx context.Context) error {
	var start time.Time
	err := backoff.Retry(func() error {
		cmd := t.newCmd()

		if err := cmd.Start(); err != nil {
			t.Log(LaunchFailed{Err: err})
			return errors.Wrap(err, "start process")
		}
		start = time.Now()
		t.Log(Launched{Process: cmd.Process})

		stopped := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				cmd.Process.Signal(os.Interrupt)
				select {
				case <-stopped:
					return
				case <-time.After(t.StopGracePeriod):
					cmd.Process.Kill()
				}
			case <-stopped:
				return
			}
		}()
		err := cmd.Wait()
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
	}, backoff.WithContext(t.Backoff, ctx))

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

func (l Launched) String() string {
	return fmt.Sprintf("launched pid %d", l.Process.Pid)
}

type LaunchFailed struct {
	Err error
}

func (l LaunchFailed) String() string { return l.Err.Error() }

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
