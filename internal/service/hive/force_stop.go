package hive

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// ForceStop performs a force-stop of Hive services
// First tries graceful stop via PID files, then kills listeners on ports 9083 and 10000
func ForceStop(pidDir string) error {
	util.Log("Force-stopping Hive (pidfiles + listeners on 9083/10000)...")

	// First try graceful stop via PID files
	stopViaPidFiles(pidDir)

	// Check if lsof is available
	if _, err := exec.LookPath("lsof"); err != nil {
		util.Warn("lsof not found; cannot force-kill listener processes.")
		return nil
	}

	// Kill listeners on Hive ports
	ports := []int{9083, 10000} // metastore, hiveserver2

	for _, port := range ports {
		pids, err := findListeners(port)
		if err != nil {
			util.Warn("Failed to find listeners on port %d: %v", port, err)
			continue
		}

		for _, pid := range pids {
			if err := killIfHive(pid, fmt.Sprintf("port %d", port)); err != nil {
				util.Warn("Failed to kill process %d: %v", pid, err)
			}
		}
	}

	// Cleanup any leftover PID files
	os.Remove(filepath.Join(pidDir, "metastore.pid"))
	os.Remove(filepath.Join(pidDir, "hiveserver2.pid"))

	return nil
}

// stopViaPidFiles attempts to stop services using PID files
func stopViaPidFiles(pidDir string) {
	services := []string{"hiveserver2", "metastore"}

	for _, svc := range services {
		pidFile := filepath.Join(pidDir, svc+".pid")
		pidBytes, err := os.ReadFile(pidFile)
		if err != nil {
			continue
		}

		pidStr := strings.TrimSpace(string(pidBytes))
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			continue
		}

		if isProcessRunning(pid) {
			killProcess(pid)
			util.Log("Stopped Hive %s (pid %d).", svc, pid)
		}

		os.Remove(pidFile)
	}
}

// findListeners finds PIDs listening on a specific port
func findListeners(port int) ([]int, error) {
	cmd := exec.Command("lsof", "-nP", fmt.Sprintf("-iTCP:%d", port), "-sTCP:LISTEN")
	output, err := cmd.Output()
	if err != nil {
		// lsof returns non-zero if no matches found, which is fine
		return nil, nil
	}

	lines := strings.Split(string(output), "\n")
	pids := make([]int, 0)

	// Skip header line
	for i, line := range lines {
		if i == 0 || line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			pid, err := strconv.Atoi(fields[1])
			if err == nil {
				pids = append(pids, pid)
			}
		}
	}

	return uniquePids(pids), nil
}

// killIfHive kills a process only if it looks like a Hive process
func killIfHive(pid int, reason string) error {
	if !isProcessRunning(pid) {
		return nil
	}

	// Get process command line
	cmd := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
	output, err := cmd.Output()
	if err != nil {
		util.Warn("Could not inspect pid %d; skipping.", pid)
		return nil
	}

	cmdLine := string(output)

	// Safety: only kill if it looks like a Hive process
	hivePatterns := []string{
		"HiveMetaStore",
		"HiveServer2",
		"hiveserver2",
		"org.apache.hadoop.hive",
	}

	isHive := false
	for _, pattern := range hivePatterns {
		if strings.Contains(cmdLine, pattern) {
			isHive = true
			break
		}
	}

	if !isHive {
		util.Warn("pid %d is listening but doesn't look like Hive; not killing.", pid)
		util.Warn("      cmd: %s", strings.TrimSpace(cmdLine))
		return nil
	}

	util.Log("Killing Hive process (pid %d) from %s", pid, reason)

	// Send SIGTERM
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	// Wait for graceful shutdown (up to 2 seconds)
	tries := 10
	for tries > 0 && isProcessRunning(pid) {
		time.Sleep(200 * time.Millisecond)
		tries--
	}

	// If still running, escalate to SIGKILL
	if isProcessRunning(pid) {
		util.Log("Escalating: kill -9 pid %d", pid)
		process.Signal(syscall.SIGKILL)
	}

	return nil
}

// isProcessRunning checks if a process is running
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// killProcess sends SIGTERM to a process
func killProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	return process.Signal(syscall.SIGTERM)
}

// uniquePids returns unique PIDs from a slice
func uniquePids(pids []int) []int {
	seen := make(map[int]bool)
	result := make([]int, 0)

	for _, pid := range pids {
		if !seen[pid] {
			seen[pid] = true
			result = append(result, pid)
		}
	}

	return result
}
