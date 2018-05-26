package exec

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// A Task holds the parameters for running a binary.
//
// It must not be modified after starting and must not be re-used after cancellation.
type Task struct {
	Binary string            // Path to the binary
	Args   []string          // Arguments
	Env    map[string]string // Environment variables
	Stdin  io.Reader         // Defaults to empty
	Stdout io.Writer         // Defaults to this program's stdout
	Stderr io.Writer         // Defaults to this program's stderr

	RestartOnSuccess bool // Whether to restart if it exits with status code 0

	// Backoff will be consulted when restarting. The default is capped at a
	// maximum interval of 15 seconds and never gives up.
	Backoff *backoff.ExponentialBackOff

	Log EventLogger // Defaults to a new StderrEventLogger

	id    string
	cmd   *exec.Cmd
	start time.Time

	discontinue chan struct{}
	done        chan struct{}
}

func NewTask(binary string, args ...string) *Task {
	t := &Task{
		Binary: binary,
		Args:   args,

		id: uuid.New().String(),

		discontinue: make(chan struct{}, 1),
		done:        make(chan struct{}),
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

	if t.Log == nil {
		t.Log = StderrEventLogger{Prefix: "exec " + t.id}
	}

	return t
}

// Admit schedules the task to run.
//
// If it exits, and its status code indicated failure or t.RestartOnSuccess is
// true, it will be restarted after an exponential backoff delay.
//
// If on first launch the process fails within the abort time, Admit returns
// an Exited event. Otherwise, Admit returns an error encountered while
// launching the process, or nil if the process continued running after the
// abort period or exited successfully within the abort period. (i.e., Admit
// will block for at most the duration of abort.)
func (t Task) Admit(abort time.Duration) error {
	if t.Backoff == nil {
		t.Backoff = backoff.NewExponentialBackOff()
		t.Backoff.MaxInterval = 15 * time.Second
		t.Backoff.MaxElapsedTime = 0
	}

	endfast := make(chan error)
	go backoff.Retry(func() error {
		t.cmd = t.newCmd()
		t.start = time.Now()

		if err := t.cmd.Start(); err != nil {
			t.Log.Log(LaunchFailed{error: err})
			endfast <- err
			return errors.New("") // retry
		}
		t.Log.Log(Launched{Process: t.cmd.Process})

		err := t.cmd.Wait()
		event := Exited{Err: err, Runtime: time.Now().Sub(t.start)}
		t.Log.Log(event)
		endfast <- event

		select {
		case <-t.discontinue:
			t.done <- struct{}{}
			return nil // cancelled, stop retrying
		default:
			if err != nil || t.RestartOnSuccess {
				return errors.New("") // retry
			}
			return nil // exited successfully, stop retrying
		}
	}, t.Backoff)

	select {
	case err := <-endfast:
		return err
	case <-time.After(abort):
		return nil
	}
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

// Cancel stops and deschedules the task. It first sends SIGINT to the task,
// if running. If it does not exit within the wait time, it is killed with
// SIGKILL.
func (t Task) Cancel(wait time.Duration) error {
	select {
	case <-t.done:
		return nil
	case t.discontinue <- struct{}{}:
		break
	default:
		return errors.New("Cancel() already called on this Task")
	}

	if t.cmd != nil {
		if err := t.cmd.Process.Signal(os.Interrupt); err != nil {
			return err
		}
	}

	select {
	case <-t.done:
		return nil
	case <-time.After(wait):
		if t.cmd != nil {
			return t.cmd.Process.Kill()
		} else {
			return nil
		}
	}
}

type EventLogger interface {
	Log(e Event) // Any given Task will only call Log sequentially
}

type Event interface {
	String() string
}

type Launched struct {
	Process *os.Process
}

func (Launched) String() string {
	return "launched"
}

type LaunchFailed struct {
	error
}

func (l LaunchFailed) String() string { return l.Error() }

type Exited struct {
	Err     error
	Runtime time.Duration
}

func (e Exited) String() string {
	return fmt.Sprintf("process ended after %v: %s", e.Runtime.Round(time.Millisecond), e.Error())
}

func (e Exited) Error() string {
	if e.Err == nil {
		return "exited successfully"
	} else {
		return e.Err.Error()
	}
}

type StderrEventLogger struct {
	Prefix string
}

func (s StderrEventLogger) Log(e Event) {
	log.Printf("exec %s: %s", s.Prefix, e.String())
}
