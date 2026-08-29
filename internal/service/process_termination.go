package service

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// ProcessAliveChecker reports whether a PID still appears to be running.
type ProcessAliveChecker func(pid int) bool

// ProcessStateChecker reports whether a PID is running, or why its state could
// not be determined. Stop paths use this form so an inspection failure cannot
// be mistaken for a confirmed exit.
type ProcessStateChecker func(pid int) (bool, error)

// ProcessSignaler sends a signal to a PID.
type ProcessSignaler func(pid int, signal syscall.Signal) error

// TerminateOptions controls graceful process termination. Tests can inject the
// signaler and liveness check to verify signal ordering without real processes.
type TerminateOptions struct {
	Timeout      time.Duration
	PollInterval time.Duration
	IsRunning    ProcessAliveChecker
	CheckRunning ProcessStateChecker
	Signal       ProcessSignaler
	Now          func() time.Time
	Sleep        func(time.Duration)
}

// TerminatePID sends SIGTERM, waits up to timeout, then escalates to SIGKILL
// only if the process remains alive.
func TerminatePID(pid int, timeout time.Duration, isRunning ProcessAliveChecker) error {
	return TerminatePIDWithOptions(pid, TerminateOptions{
		Timeout:   timeout,
		IsRunning: isRunning,
	})
}

// TerminatePIDWithOptions is the injectable form of TerminatePID.
func TerminatePIDWithOptions(pid int, options TerminateOptions) error {
	if pid <= 0 {
		return nil
	}

	if options.CheckRunning == nil && options.IsRunning == nil {
		options.CheckRunning = inspectProcessRunning
	}
	if options.Signal == nil {
		options.Signal = signalProcess
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Sleep == nil {
		options.Sleep = time.Sleep
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	if options.Timeout < 0 {
		options.Timeout = 0
	}

	running, err := processRunning(options, pid)
	if err != nil {
		return fmt.Errorf("failed to inspect pid %d before SIGTERM: %w", pid, err)
	}
	if !running {
		return nil
	}

	if err := signalAndConfirmRace(pid, syscall.SIGTERM, options); err != nil {
		return err
	}
	exited, err := waitForProcessExit(pid, options)
	if err != nil {
		return fmt.Errorf("failed to confirm pid %d exited after SIGTERM: %w", pid, err)
	}
	if exited {
		return nil
	}

	running, err = processRunning(options, pid)
	if err != nil {
		return fmt.Errorf("failed to inspect pid %d before SIGKILL: %w", pid, err)
	}
	if !running {
		return nil
	}
	if err := signalAndConfirmRace(pid, syscall.SIGKILL, options); err != nil {
		return fmt.Errorf("after SIGTERM timeout: %w", err)
	}
	exited, err = waitForProcessExit(pid, options)
	if err != nil {
		return fmt.Errorf("failed to confirm pid %d exited after SIGKILL: %w", pid, err)
	}
	if exited {
		return nil
	}

	return fmt.Errorf("pid %d still running after SIGKILL", pid)
}

func processRunning(options TerminateOptions, pid int) (bool, error) {
	if options.CheckRunning != nil {
		return options.CheckRunning(pid)
	}
	if options.IsRunning != nil {
		return options.IsRunning(pid), nil
	}
	return inspectProcessRunning(pid)
}

func signalAndConfirmRace(pid int, signal syscall.Signal, options TerminateOptions) error {
	err := options.Signal(pid, signal)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("failed to send %s to pid %d: %w", signalName(signal), pid, err)
	}

	running, inspectErr := processRunning(options, pid)
	if inspectErr != nil {
		return errors.Join(
			fmt.Errorf("failed to send %s to pid %d: %w", signalName(signal), pid, err),
			fmt.Errorf("failed to confirm pid %d exited: %w", pid, inspectErr),
		)
	}
	if running {
		return fmt.Errorf("failed to send %s to pid %d: %w", signalName(signal), pid, err)
	}
	return nil
}

func signalName(signal syscall.Signal) string {
	switch signal {
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGKILL:
		return "SIGKILL"
	default:
		return signal.String()
	}
}

func signalProcess(pid int, signal syscall.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	return process.Signal(signal)
}

func waitForProcessExit(pid int, options TerminateOptions) (bool, error) {
	deadline := options.Now().Add(options.Timeout)
	for {
		running, err := processRunning(options, pid)
		if err != nil {
			return false, err
		}
		if !running {
			return true, nil
		}
		if !options.Now().Before(deadline) {
			return false, nil
		}

		sleepFor := options.PollInterval
		if remaining := deadline.Sub(options.Now()); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor > 0 {
			options.Sleep(sleepFor)
		}
	}
}
