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

// ProcessSignaler sends a signal to a PID.
type ProcessSignaler func(pid int, signal syscall.Signal) error

// TerminateOptions controls graceful process termination. Tests can inject the
// signaler and liveness check to verify signal ordering without real processes.
type TerminateOptions struct {
	Timeout      time.Duration
	PollInterval time.Duration
	IsRunning    ProcessAliveChecker
	Signal       ProcessSignaler
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

	if options.IsRunning == nil {
		options.IsRunning = isProcessRunning
	}
	if options.Signal == nil {
		options.Signal = signalProcess
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	if options.Timeout < 0 {
		options.Timeout = 0
	}

	if !options.IsRunning(pid) {
		return nil
	}

	if err := options.Signal(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to send SIGTERM to pid %d: %w", pid, err)
	}
	if waitForProcessExit(pid, options) {
		return nil
	}

	if !options.IsRunning(pid) {
		return nil
	}
	if err := options.Signal(pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("failed to send SIGKILL to pid %d after SIGTERM timeout: %w", pid, err)
	}
	if waitForProcessExit(pid, options) {
		return nil
	}

	return fmt.Errorf("pid %d still running after SIGKILL", pid)
}

func signalProcess(pid int, signal syscall.Signal) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	if err := process.Signal(signal); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}

	return nil
}

func waitForProcessExit(pid int, options TerminateOptions) bool {
	deadline := time.Now().Add(options.Timeout)
	for {
		if !options.IsRunning(pid) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}

		sleepFor := options.PollInterval
		if remaining := time.Until(deadline); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor > 0 {
			time.Sleep(sleepFor)
		}
	}
}
