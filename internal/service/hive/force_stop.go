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

// ForceStop performs a force-stop of Hive services.
// First tries graceful stop via PID files, then kills configured Hive listener ports.
func ForceStop(pidDir string, ports ...int) error {
	listenerPorts := forceStopListenerPorts(pidDir, ports...)
	util.Log("Force-stopping Hive (pidfiles + listeners on %s)...", formatPorts(listenerPorts))

	// First try graceful stop via PID files
	stopViaPidFiles(pidDir)

	// Check if lsof is available
	if _, err := exec.LookPath("lsof"); err != nil {
		util.Warn("lsof not found; cannot force-kill listener processes.")
		return nil
	}

	for _, port := range listenerPorts {
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
	removeFile(filepath.Join(pidDir, "metastore.pid"))
	removeFile(filepath.Join(pidDir, "hiveserver2.pid"))

	return nil
}

func forceStopListenerPorts(pidDir string, ports ...int) []int {
	if len(ports) > 0 {
		return normalizePorts(ports)
	}

	baseDir := filepath.Dir(filepath.Dir(filepath.Dir(pidDir)))
	hiveSite := filepath.Join(baseDir, "conf", "current", "hive", "hive-site.xml")
	return normalizePorts(readHiveListenerPorts(hiveSite).slice())
}

func normalizePorts(ports []int) []int {
	seen := make(map[int]bool)
	result := make([]int, 0, len(ports))

	for _, port := range ports {
		if port <= 0 || port > 65535 || seen[port] {
			continue
		}
		seen[port] = true
		result = append(result, port)
	}

	if len(result) == 0 {
		return defaultHivePorts().slice()
	}

	return result
}

func formatPorts(ports []int) string {
	parts := make([]string, 0, len(ports))
	for _, port := range ports {
		parts = append(parts, strconv.Itoa(port))
	}
	return strings.Join(parts, "/")
}

func removeFile(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		util.Warn("Failed to remove %s: %v", path, err)
	}
}

// stopViaPidFiles attempts to stop services using PID files
func stopViaPidFiles(pidDir string) {
	services := []string{"hiveserver2", "metastore"}
	validate := hivePIDValidator()

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
			if err := validate(svc, pid); err != nil {
				util.Warn("Skipping Hive %s PID file stop: %v", svc, err)
			} else if err := killIfHive(pid, fmt.Sprintf("PID file %s", svc)); err != nil {
				util.Warn("Failed to kill Hive %s from PID file: %v", svc, err)
			} else {
				util.Success("Stopped Hive %s (pid %d).", svc, pid)
			}
		}

		removeFile(pidFile)
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
		if err := process.Signal(syscall.SIGKILL); err != nil {
			return err
		}
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
