package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ProcessManager handles process lifecycle management
// Manages PID files, process start/stop, and status checking
type ProcessManager struct {
	PidDir           string       // Directory for PID files
	LogDir           string       // Directory for log files
	ValidatePID      PIDValidator // Optional ownership check before signaling PID-file processes
	StopTimeout      time.Duration
	StopPollInterval time.Duration
	CheckRunning     ProcessStateChecker
	Signal           ProcessSignaler
	Now              func() time.Time
	Sleep            func(time.Duration)

	hooks      processManagerHooks
	childrenMu sync.Mutex
	children   map[int]*exec.Cmd
}

type startedProcessChecker func(cmd *exec.Cmd) (alive bool, reaped bool, err error)

type processManagerHooks struct {
	closeLogFile          func(*os.File) error
	writePIDFile          func(string, []byte, os.FileMode) error
	removePIDFile         func(string) error
	checkStartedProcess   startedProcessChecker
	cleanupStartedProcess func(*exec.Cmd) error
}

const (
	defaultStartGracePeriod  = time.Second
	defaultStartPollInterval = 25 * time.Millisecond
	defaultStopTimeout       = 5 * time.Second
	defaultStopPollInterval  = 100 * time.Millisecond
)

// NewProcessManager creates a new process manager
func NewProcessManager(pidDir, logDir string) *ProcessManager {
	return &ProcessManager{
		PidDir: pidDir,
		LogDir: logDir,
	}
}

// Start starts a process and writes its PID to a file
// Mirrors Bash: nohup cmd > log 2>&1 &
func (pm *ProcessManager) Start(name string, cmd *exec.Cmd, logFile string) (int, error) {
	// Ensure directories exist
	if err := os.MkdirAll(pm.PidDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create PID directory: %w", err)
	}
	if err := os.MkdirAll(pm.LogDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open log file
	logPath := filepath.Join(pm.LogDir, logFile)
	logf, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open log file: %w", err)
	}

	// Redirect stdout and stderr to log file
	cmd.Stdout = logf
	cmd.Stderr = logf

	// Start the process (non-blocking)
	if err := cmd.Start(); err != nil {
		if closeErr := logf.Close(); closeErr != nil {
			return 0, fmt.Errorf("failed to start process: %w; additionally failed to close log file: %v", err, closeErr)
		}
		return 0, fmt.Errorf("failed to start process: %w", err)
	}

	pid := cmd.Process.Pid

	// Close log file in parent (child has its own descriptor)
	if err := pm.closeLogFile()(logf); err != nil {
		return 0, pm.failStartedProcess(cmd, "", false, fmt.Errorf("failed to close log file: %w", err))
	}

	// Publish the PID atomically so readers never observe partial contents.
	pidPath := filepath.Join(pm.PidDir, name+".pid")
	if err := pm.writePIDFile()(pidPath, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return 0, pm.failStartedProcess(cmd, pidPath, false, fmt.Errorf("failed to publish PID file: %w", err))
	}

	// Verify the exact child, rather than only probing its PID. kill(0) reports a
	// zombie as alive; wait4 with WNOHANG detects and reaps an immediate exit.
	reaped, err := pm.verifyStartedProcess(cmd)
	if err != nil {
		primary := fmt.Errorf("process %s failed startup verification (check logs: %s): %w", name, logPath, err)
		return 0, pm.failStartedProcess(cmd, pidPath, reaped, primary)
	}
	pm.trackChild(cmd)

	return pid, nil
}

// Stop stops a process by reading its PID file and sending SIGTERM
func (pm *ProcessManager) Stop(name string) error {
	pidPath := filepath.Join(pm.PidDir, name+".pid")

	// Read PID file
	if _, err := os.Stat(pidPath); os.IsNotExist(err) {
		// PID file doesn't exist, process not running
		return nil
	}

	data, err := os.ReadFile(pidPath)
	if err != nil {
		return fmt.Errorf("failed to read PID file: %w", err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("invalid PID in file: %w", err)
	}

	running, err := pm.checkRunning()(pid)
	if err != nil {
		return fmt.Errorf("failed to inspect %s pid %d: %w", name, pid, err)
	}
	if running {
		signal := pm.signaler()
		if pm.ValidatePID != nil {
			signal = func(pid int, sig syscall.Signal) error {
				if err := pm.ValidatePID(name, pid); err != nil {
					return err
				}
				return pm.signaler()(pid, sig)
			}
		}

		if err := TerminatePIDWithOptions(pid, TerminateOptions{
			Timeout:      pm.stopTimeout(),
			PollInterval: pm.stopPollInterval(),
			CheckRunning: pm.checkRunning(),
			Signal:       signal,
			Now:          pm.now(),
			Sleep:        pm.sleep(),
		}); err != nil {
			return fmt.Errorf("failed to stop %s pid %d: %w", name, pid, err)
		}
	}
	pm.untrackChild(pid)

	// The ownership record is removed only after death has been confirmed.
	if err := os.Remove(pidPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove PID file: %w", err)
	}

	return nil
}

// Status returns the PID if the process is running, 0 otherwise
func (pm *ProcessManager) Status(name string) (int, error) {
	pidPath := filepath.Join(pm.PidDir, name+".pid")

	// Check if PID file exists
	if _, err := os.Stat(pidPath); os.IsNotExist(err) {
		return 0, nil
	}

	// Read PID
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read PID file: %w", err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in file: %w", err)
	}

	// Check if process is running
	running, err := pm.checkRunning()(pid)
	if err != nil {
		return 0, fmt.Errorf("failed to inspect pid %d: %w", pid, err)
	}
	if running {
		return pid, nil
	}

	// Process not running, clean up stale PID file
	if err := os.Remove(pidPath); err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("failed to remove stale PID file: %w", err)
	}
	return 0, nil
}

// isProcessRunning checks if a process with the given PID is running
// Uses kill -0 signal to check without actually killing the process
func isProcessRunning(pid int) bool {
	running, err := inspectProcessRunning(pid)
	if err != nil {
		// Status callers historically treated EPERM and similar errors as proof
		// that a process exists. Stop uses the error-returning checker instead.
		return true
	}
	return running
}

func inspectProcessRunning(pid int) (bool, error) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false, err
	}

	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true, nil
	}

	// ESRCH means process doesn't exist
	if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return false, nil
	}

	return false, err
}

// IsRunning checks if a named process is currently running
func (pm *ProcessManager) IsRunning(name string) bool {
	pid, _ := pm.Status(name)
	return pid != 0
}

func (pm *ProcessManager) closeLogFile() func(*os.File) error {
	if pm.hooks.closeLogFile != nil {
		return pm.hooks.closeLogFile
	}
	return func(file *os.File) error { return file.Close() }
}

func (pm *ProcessManager) writePIDFile() func(string, []byte, os.FileMode) error {
	if pm.hooks.writePIDFile != nil {
		return pm.hooks.writePIDFile
	}
	return writePIDFileAtomic
}

func (pm *ProcessManager) removePIDFile(path string) error {
	remove := pm.hooks.removePIDFile
	if remove == nil {
		remove = os.Remove
	}
	if err := remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (pm *ProcessManager) failStartedProcess(cmd *exec.Cmd, pidPath string, alreadyReaped bool, primary error) error {
	var cleanupErr error
	if !alreadyReaped {
		cleanup := pm.hooks.cleanupStartedProcess
		if cleanup == nil {
			cleanup = terminateAndReapStartedProcess
		}
		if err := cleanup(cmd); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("failed to terminate and reap started process: %w", err))
		}
	}
	if pidPath != "" {
		if err := pm.removePIDFile(pidPath); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("failed to remove PID file after start failure: %w", err))
		}
	}
	return errors.Join(primary, cleanupErr)
}

func (pm *ProcessManager) verifyStartedProcess(cmd *exec.Cmd) (bool, error) {
	check := pm.hooks.checkStartedProcess
	if check == nil {
		check = checkStartedProcess
	}
	deadline := pm.now()().Add(defaultStartGracePeriod)
	for {
		alive, reaped, err := check(cmd)
		if err != nil {
			return reaped, err
		}
		if !alive {
			return reaped, errors.New("child exited during startup")
		}
		if !pm.now()().Before(deadline) {
			return false, nil
		}
		sleepFor := defaultStartPollInterval
		if remaining := deadline.Sub(pm.now()()); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor > 0 {
			pm.sleep()(sleepFor)
		}
	}
}

func checkStartedProcess(cmd *exec.Cmd) (bool, bool, error) {
	var status syscall.WaitStatus
	waitedPID, err := syscall.Wait4(cmd.Process.Pid, &status, syscall.WNOHANG, nil)
	if err != nil {
		return false, false, fmt.Errorf("could not inspect child pid %d: %w", cmd.Process.Pid, err)
	}
	if waitedPID == 0 {
		return true, false, nil
	}
	if waitedPID != cmd.Process.Pid {
		return false, false, fmt.Errorf("waited for unexpected child pid %d while checking pid %d", waitedPID, cmd.Process.Pid)
	}
	if status.Exited() {
		return false, true, fmt.Errorf("child pid %d exited with status %d", waitedPID, status.ExitStatus())
	}
	if status.Signaled() {
		return false, true, fmt.Errorf("child pid %d exited after signal %s", waitedPID, status.Signal())
	}
	return false, true, fmt.Errorf("child pid %d exited during startup", waitedPID)
}

func terminateAndReapStartedProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) || errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ECHILD) {
			return nil
		}
		return err
	}
	return nil
}

func writePIDFileAtomic(path string, data []byte, mode os.FileMode) (err error) {
	tempFile, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer func() {
		if tempFile != nil {
			_ = tempFile.Close()
		}
		_ = os.Remove(tempPath)
	}()

	if err := tempFile.Chmod(mode); err != nil {
		return err
	}
	if _, err := tempFile.Write(data); err != nil {
		return err
	}
	if err := tempFile.Sync(); err != nil {
		return err
	}
	if err := tempFile.Close(); err != nil {
		return err
	}
	tempFile = nil
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}
	return nil
}

func (pm *ProcessManager) checkRunning() ProcessStateChecker {
	if pm.CheckRunning != nil {
		return pm.CheckRunning
	}
	return pm.inspectManagedProcessRunning
}

func (pm *ProcessManager) trackChild(cmd *exec.Cmd) {
	pm.childrenMu.Lock()
	defer pm.childrenMu.Unlock()
	if pm.children == nil {
		pm.children = make(map[int]*exec.Cmd)
	}
	pm.children[cmd.Process.Pid] = cmd
}

func (pm *ProcessManager) untrackChild(pid int) {
	pm.childrenMu.Lock()
	defer pm.childrenMu.Unlock()
	delete(pm.children, pid)
}

func (pm *ProcessManager) inspectManagedProcessRunning(pid int) (bool, error) {
	pm.childrenMu.Lock()
	cmd := pm.children[pid]
	if cmd != nil {
		var status syscall.WaitStatus
		waitedPID, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		switch {
		case err == nil && waitedPID == 0:
			pm.childrenMu.Unlock()
			return true, nil
		case err == nil && waitedPID == pid:
			delete(pm.children, pid)
			pm.childrenMu.Unlock()
			return false, nil
		case errors.Is(err, syscall.ECHILD):
			delete(pm.children, pid)
			pm.childrenMu.Unlock()
			return inspectProcessRunning(pid)
		case err != nil:
			pm.childrenMu.Unlock()
			return false, err
		default:
			pm.childrenMu.Unlock()
			return false, fmt.Errorf("waited for unexpected child pid %d while checking pid %d", waitedPID, pid)
		}
	}
	pm.childrenMu.Unlock()
	return inspectProcessRunning(pid)
}

func (pm *ProcessManager) signaler() ProcessSignaler {
	if pm.Signal != nil {
		return pm.Signal
	}
	return signalProcess
}

func (pm *ProcessManager) stopTimeout() time.Duration {
	if pm.StopTimeout > 0 {
		return pm.StopTimeout
	}
	return defaultStopTimeout
}

func (pm *ProcessManager) stopPollInterval() time.Duration {
	if pm.StopPollInterval > 0 {
		return pm.StopPollInterval
	}
	return defaultStopPollInterval
}

func (pm *ProcessManager) now() func() time.Time {
	if pm.Now != nil {
		return pm.Now
	}
	return time.Now
}

func (pm *ProcessManager) sleep() func(time.Duration) {
	if pm.Sleep != nil {
		return pm.Sleep
	}
	return time.Sleep
}
