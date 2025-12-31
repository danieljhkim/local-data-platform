package service

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// ProcessManager handles process lifecycle management
// Manages PID files, process start/stop, and status checking
type ProcessManager struct {
	PidDir string // Directory for PID files
	LogDir string // Directory for log files
}

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
		logf.Close()
		return 0, fmt.Errorf("failed to start process: %w", err)
	}

	pid := cmd.Process.Pid

	// Close log file in parent (child has its own descriptor)
	logf.Close()

	// Write PID file
	pidPath := filepath.Join(pm.PidDir, name+".pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return 0, fmt.Errorf("failed to write PID file: %w", err)
	}

	// Verify process stayed alive
	time.Sleep(1 * time.Second)
	if !isProcessRunning(pid) {
		return 0, fmt.Errorf("process %s failed to stay running (check logs: %s)", name, logPath)
	}

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

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return fmt.Errorf("invalid PID in file: %w", err)
	}

	// Send SIGTERM
	if isProcessRunning(pid) {
		process, err := os.FindProcess(pid)
		if err != nil {
			return fmt.Errorf("failed to find process: %w", err)
		}

		if err := process.Signal(syscall.SIGTERM); err != nil {
			// Process might have already exited
			if err != syscall.ESRCH {
				return fmt.Errorf("failed to send SIGTERM: %w", err)
			}
		}
	}

	// Remove PID file
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

	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in file: %w", err)
	}

	// Check if process is running
	if isProcessRunning(pid) {
		return pid, nil
	}

	// Process not running, clean up stale PID file
	os.Remove(pidPath)
	return 0, nil
}

// isProcessRunning checks if a process with the given PID is running
// Uses kill -0 signal to check without actually killing the process
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}

	// ESRCH means process doesn't exist
	if err == syscall.ESRCH {
		return false
	}

	// Other errors (like EPERM) mean process exists but we can't signal it
	return true
}

// IsRunning checks if a named process is currently running
func (pm *ProcessManager) IsRunning(name string) bool {
	pid, _ := pm.Status(name)
	return pid != 0
}
