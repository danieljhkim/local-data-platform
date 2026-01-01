package yarn

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// YARNService manages the YARN ResourceManager and NodeManager services
type YARNService struct {
	paths   *config.Paths
	env     *env.Environment
	procMgr *service.ProcessManager
}

// NewYARNService creates a new YARN service manager
func NewYARNService(paths *config.Paths) (*YARNService, error) {
	environment, err := env.Compute(paths)
	if err != nil {
		return nil, fmt.Errorf("failed to compute environment: %w", err)
	}

	stateDir := filepath.Join(paths.StateDir(), "yarn")
	pidDir := filepath.Join(stateDir, "pids")
	logDir := filepath.Join(stateDir, "logs")

	if err := util.MkdirAll(pidDir, logDir); err != nil {
		return nil, fmt.Errorf("failed to create YARN directories: %w", err)
	}

	procMgr := &service.ProcessManager{
		PidDir: pidDir,
		LogDir: logDir,
	}

	return &YARNService{
		paths:   paths,
		env:     environment,
		procMgr: procMgr,
	}, nil
}

// Start starts the YARN ResourceManager and NodeManager
func (y *YARNService) Start() error {
	util.Log("Starting YARN services...")

	// Start ResourceManager
	if err := y.startResourceManager(); err != nil {
		return err
	}

	// Start NodeManager
	if err := y.startNodeManager(); err != nil {
		return err
	}

	return nil
}

// startResourceManager starts the YARN ResourceManager
func (y *YARNService) startResourceManager() error {
	name := "resourcemanager"

	// Check if already running
	pid, err := y.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("YARN ResourceManager already running (pid %d).", pid)
		return nil
	}

	// Try to find via jps
	pid = findWithJPS("ResourceManager")
	if pid > 0 && isProcessRunning(pid) {
		pidFile := filepath.Join(y.procMgr.PidDir, name+".pid")
		os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644)
		util.Log("YARN ResourceManager already running (pid %d).", pid)
		return nil
	}

	// Start the ResourceManager
	cmd := exec.Command("yarn", "resourcemanager")
	cmd.Env = y.env.Export()

	logFile := name + ".log"
	startedPid, err := y.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return fmt.Errorf("failed to start ResourceManager: %w", err)
	}

	util.Log("YARN ResourceManager started (pid %d).", startedPid)
	return nil
}

// startNodeManager starts the YARN NodeManager
func (y *YARNService) startNodeManager() error {
	name := "nodemanager"

	// Check if already running
	pid, err := y.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("YARN NodeManager already running (pid %d).", pid)
		return nil
	}

	// Try to find via jps
	pid = findWithJPS("NodeManager")
	if pid > 0 && isProcessRunning(pid) {
		pidFile := filepath.Join(y.procMgr.PidDir, name+".pid")
		os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644)
		util.Log("YARN NodeManager already running (pid %d).", pid)
		return nil
	}

	// Start the NodeManager
	cmd := exec.Command("yarn", "nodemanager")
	cmd.Env = y.env.Export()

	logFile := name + ".log"
	startedPid, err := y.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return fmt.Errorf("failed to start NodeManager: %w", err)
	}

	util.Log("YARN NodeManager started (pid %d).", startedPid)
	return nil
}

// Stop stops the YARN ResourceManager and NodeManager
func (y *YARNService) Stop() error {
	util.Log("Stopping YARN services...")

	// Stop in reverse order: NodeManager, then ResourceManager
	services := []struct {
		name      string
		className string
	}{
		{"nodemanager", "NodeManager"},
		{"resourcemanager", "ResourceManager"},
	}

	for _, svc := range services {
		// Try to stop via PID file
		pid, err := y.procMgr.Status(svc.name)
		if err == nil && pid > 0 {
			if err := y.procMgr.Stop(svc.name); err != nil {
				util.Warn("Failed to stop YARN %s via PID file: %v", svc.name, err)
			} else {
				util.Log("Stopped YARN %s (pid %d).", svc.name, pid)
				continue
			}
		}

		// Fallback: try to find via jps
		jpsPid := findWithJPS(svc.className)
		if jpsPid > 0 && isProcessRunning(jpsPid) {
			if err := killProcess(jpsPid); err != nil {
				util.Warn("Failed to stop YARN %s via jps: %v", svc.name, err)
			} else {
				util.Log("Stopped YARN %s (pid %d) via jps.", svc.name, jpsPid)
			}
		}

		// Clean up PID file
		pidFile := filepath.Join(y.procMgr.PidDir, svc.name+".pid")
		os.Remove(pidFile)
	}

	return nil
}

// Status returns the status of YARN services
func (y *YARNService) Status() ([]service.ServiceStatus, error) {
	services := []struct {
		name      string
		className string
	}{
		{"resourcemanager", "ResourceManager"},
		{"nodemanager", "NodeManager"},
	}

	statuses := make([]service.ServiceStatus, 0, len(services))

	for _, svc := range services {
		status := service.ServiceStatus{Name: svc.name}

		// Check PID file first
		pid, err := y.procMgr.Status(svc.name)
		if err == nil && pid > 0 {
			status.Running = true
			status.PID = pid
		} else {
			// Fallback: try jps
			jpsPid := findWithJPS(svc.className)
			if jpsPid > 0 && isProcessRunning(jpsPid) {
				status.Running = true
				status.PID = jpsPid
			}
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

// Logs displays YARN service logs
func (y *YARNService) Logs() error {
	logDir := y.procMgr.LogDir

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return fmt.Errorf("no YARN logs directory found: %s (have you started YARN?)", logDir)
	}

	logFiles := []string{
		filepath.Join(logDir, "resourcemanager.log"),
		filepath.Join(logDir, "nodemanager.log"),
	}

	for _, logFile := range logFiles {
		fmt.Printf("==> %s\n", logFile)
		if _, err := os.Stat(logFile); err == nil {
			cmd := exec.Command("tail", "-n", "120", logFile)
			cmd.Stdout = os.Stdout
			_ = cmd.Run()
		} else {
			fmt.Println("(missing)")
		}
		fmt.Println()
	}

	return nil
}

// findWithJPS finds a process by Java class name using jps
func findWithJPS(className string) int {
	cmd := exec.Command("jps", "-l")
	output, err := cmd.Output()
	if err != nil {
		return 0
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, className) {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				pid, err := strconv.Atoi(fields[0])
				if err == nil {
					return pid
				}
			}
		}
	}

	return 0
}

// isProcessRunning checks if a process is running using kill -0
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(os.Signal(nil))
	return err == nil
}

// killProcess sends SIGTERM to a process
func killProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	// Send SIGTERM
	if err := process.Kill(); err != nil {
		return err
	}

	// Wait a bit for graceful shutdown
	time.Sleep(500 * time.Millisecond)

	return nil
}
