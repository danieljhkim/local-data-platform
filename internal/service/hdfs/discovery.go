package hdfs

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// findWithJPS finds a Java process using jps command
// Mirrors ld_hdfs_jps_pid
func findWithJPS(className string) (int, error) {
	// Check if jps is available
	if _, err := exec.LookPath("jps"); err != nil {
		return 0, nil // Not found, not an error
	}

	cmd := exec.Command("jps", "-l")
	output, err := cmd.Output()
	if err != nil {
		return 0, nil // jps failed, not an error
	}

	// Parse jps output
	// Format: <pid> <fully.qualified.ClassName>
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			// Check if class name contains our target
			if strings.Contains(fields[1], className) {
				pid, err := strconv.Atoi(fields[0])
				if err == nil {
					return pid, nil
				}
			}
		}
	}

	return 0, nil // Not found
}

// findWithPgrep finds a process using pgrep command
// Mirrors ld_hdfs_pgrep_pid
func findWithPgrep(pattern string) (int, error) {
	// Check if pgrep is available
	if _, err := exec.LookPath("pgrep"); err != nil {
		return 0, nil // Not found, not an error
	}

	cmd := exec.Command("pgrep", "-f", pattern)
	output, err := cmd.Output()
	if err != nil {
		return 0, nil // pgrep failed or no match, not an error
	}

	// Return first PID
	pidStr := strings.TrimSpace(string(output))
	if pidStr == "" {
		return 0, nil
	}

	// pgrep returns multiple PIDs, one per line - take the first
	lines := strings.Split(pidStr, "\n")
	if len(lines) > 0 && lines[0] != "" {
		pid, err := strconv.Atoi(lines[0])
		if err == nil {
			return pid, nil
		}
	}

	return 0, nil
}

// FindNameNodePID finds the NameNode process ID
// Uses jps first, falls back to pgrep
// Mirrors ld_hdfs_find_pid for namenode
func FindNameNodePID() (int, error) {
	// Try jps first
	pid, err := findWithJPS("NameNode")
	if err != nil || pid != 0 {
		return pid, err
	}

	// Fallback to pgrep
	return findWithPgrep(`org\.apache\.hadoop\.hdfs\.server\.namenode\.NameNode`)
}

// FindDataNodePID finds the DataNode process ID
// Uses jps first, falls back to pgrep
// Mirrors ld_hdfs_find_pid for datanode
func FindDataNodePID() (int, error) {
	// Try jps first
	pid, err := findWithJPS("DataNode")
	if err != nil || pid != 0 {
		return pid, err
	}

	// Fallback to pgrep
	return findWithPgrep(`org\.apache\.hadoop\.hdfs\.server\.datanode\.DataNode`)
}

// CheckConfOverlay checks if a process is using the correct HADOOP_CONF_DIR
// Mirrors ld_hdfs_pid_uses_current_conf
func CheckConfOverlay(pid int, expectedConfDir string) bool {
	if pid == 0 {
		return false
	}

	// Try ps with environment variables (macOS-specific)
	cmd := exec.Command("ps", "eww", "-p", strconv.Itoa(pid))
	output, err := cmd.Output()
	if err != nil {
		// Fallback to regular ps
		cmd = exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=")
		output, err = cmd.Output()
		if err != nil {
			return false
		}
	}

	// Check if output contains the expected HADOOP_CONF_DIR
	return strings.Contains(string(output), "HADOOP_CONF_DIR="+expectedConfDir)
}

// IsProcessRunning checks if a process is running using kill -0
func IsProcessRunning(pid int) bool {
	if pid == 0 {
		return false
	}

	// Use kill -0 to check if process exists
	cmd := exec.Command("kill", "-0", strconv.Itoa(pid))
	err := cmd.Run()
	return err == nil
}

// WaitForSafeMode waits for HDFS to exit safe mode
// Returns error if timeout is reached
func WaitForSafeMode(maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		cmd := exec.Command("hdfs", "dfsadmin", "-safemode", "get")
		output, err := cmd.Output()
		if err != nil {
			// Command failed, HDFS might not be ready
			time.Sleep(1 * time.Second)
			continue
		}

		outputStr := string(output)
		// Check if safe mode is OFF
		if strings.Contains(outputStr, "Safe mode is OFF") {
			return nil
		}

		// Wait 1 second before retry
		if i < maxRetries-1 {
			time.Sleep(1 * time.Second)
		}
	}

	return fmt.Errorf("HDFS did not exit safe mode after %d retries", maxRetries)
}
