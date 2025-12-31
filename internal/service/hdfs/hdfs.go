package hdfs

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// HDFSService manages the HDFS NameNode and DataNode
type HDFSService struct {
	paths   *config.Paths
	env     *env.Environment
	procMgr *service.ProcessManager
}

// NewHDFSService creates a new HDFS service manager
func NewHDFSService(paths *config.Paths) (*HDFSService, error) {
	// Compute environment
	environment, err := env.Compute(paths)
	if err != nil {
		return nil, err
	}

	// Get HDFS paths
	hdfsPaths := paths.HDFSPaths()

	// Create process manager
	procMgr := service.NewProcessManager(hdfsPaths.PidsDir, hdfsPaths.LogsDir)

	return &HDFSService{
		paths:   paths,
		env:     environment,
		procMgr: procMgr,
	}, nil
}

// Start starts the HDFS NameNode and DataNode
// Mirrors ld_hdfs_start
func (h *HDFSService) Start() error {
	// Ensure Hadoop is available
	if h.env.HadoopHome == "" {
		return fmt.Errorf("Hadoop not found (HADOOP_HOME not set). Install with: brew install hadoop")
	}

	// Ensure local storage directories exist
	if err := EnsureLocalStorageDirs(h.paths.BaseDir); err != nil {
		return err
	}

	// Ensure NameNode is formatted
	if err := EnsureNameNodeFormatted(h.env.HadoopConfDir); err != nil {
		return err
	}

	// Ensure log and PID directories exist
	hdfsPaths := h.paths.HDFSPaths()
	if err := util.MkdirAll(hdfsPaths.LogsDir, hdfsPaths.PidsDir); err != nil {
		return err
	}

	// Start NameNode
	if err := h.startNameNode(); err != nil {
		return err
	}

	// Start DataNode
	if err := h.startDataNode(); err != nil {
		return err
	}

	// Wait for safe mode to exit (increase retries for fresh format)
	util.Log("Waiting for NameNode to exit safe mode...")
	safeModeExited := true
	if err := WaitForSafeMode(10); err != nil {
		util.Warn("%v", err)
		util.Warn("NameNode may still be in safe mode. Check logs: %s", hdfsPaths.LogsDir)
		safeModeExited = false
	}

	// Create common HDFS directories
	// Try to create directories even if safe mode didn't exit, but warn about potential failures
	util.Log("Creating common HDFS directories...")
	currentUser, err := user.Current()
	username := "hadoop"
	if err == nil {
		username = currentUser.Username
	}
	if err := CreateCommonHDFSDirs(username); err != nil {
		util.Warn("Failed to create some HDFS directories: %v", err)
		if !safeModeExited {
			util.Warn("This is likely because HDFS is still in safe mode.")
			util.Warn("Run 'local-data start hdfs' again once safe mode exits,")
			util.Warn("or manually create directories with: local-data hdfs dfs -mkdir -p /tmp /user/$USER /user/hive/warehouse /spark-history")
		}
	}

	return nil
}

// startNameNode starts the NameNode process
func (h *HDFSService) startNameNode() error {
	// Check if already running
	pid, _ := h.procMgr.Status("namenode")
	if pid == 0 {
		// Try to find via jps/pgrep
		pid, _ = FindNameNodePID()
	}

	// If running, check if using current config
	if pid != 0 {
		if !CheckConfOverlay(pid, h.env.HadoopConfDir) {
			util.Log("HDFS NameNode running but not using current overlay config; restarting (pid %d).", pid)
			h.procMgr.Stop("namenode")
			time.Sleep(500 * time.Millisecond)
			pid = 0
		}
	}

	// If still running, we're done
	if pid != 0 && IsProcessRunning(pid) {
		// Update PID file
		hdfsPaths := h.paths.HDFSPaths()
		pidFile := filepath.Join(hdfsPaths.PidsDir, "namenode.pid")
		os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0644)
		util.Log("HDFS NameNode already running (pid %d).", pid)
		return nil
	}

	// Start NameNode
	cmd := exec.Command("hdfs", "namenode")
	cmd.Env = h.env.MergeWithCurrent()

	pid, err := h.procMgr.Start("namenode", cmd, "namenode.log")
	if err != nil {
		return fmt.Errorf("failed to start NameNode: %w", err)
	}

	util.Log("HDFS NameNode started (pid %d).", pid)
	return nil
}

// startDataNode starts the DataNode process
func (h *HDFSService) startDataNode() error {
	// Check if already running
	pid, _ := h.procMgr.Status("datanode")
	if pid == 0 {
		// Try to find via jps/pgrep
		pid, _ = FindDataNodePID()
	}

	// If running, check if using current config
	if pid != 0 {
		if !CheckConfOverlay(pid, h.env.HadoopConfDir) {
			util.Log("HDFS DataNode running but not using current overlay config; restarting (pid %d).", pid)
			h.procMgr.Stop("datanode")
			time.Sleep(500 * time.Millisecond)
			pid = 0
		}
	}

	// If still running, we're done
	if pid != 0 && IsProcessRunning(pid) {
		// Update PID file
		hdfsPaths := h.paths.HDFSPaths()
		pidFile := filepath.Join(hdfsPaths.PidsDir, "datanode.pid")
		os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0644)
		util.Log("HDFS DataNode already running (pid %d).", pid)
		return nil
	}

	// Start DataNode
	cmd := exec.Command("hdfs", "datanode")
	cmd.Env = h.env.MergeWithCurrent()

	pid, err := h.procMgr.Start("datanode", cmd, "datanode.log")
	if err != nil {
		return fmt.Errorf("failed to start DataNode: %w", err)
	}

	util.Log("HDFS DataNode started (pid %d).", pid)
	return nil
}

// Stop stops the HDFS NameNode and DataNode
// Mirrors ld_hdfs_stop
func (h *HDFSService) Stop() error {
	// Stop in reverse order: DataNode first, then NameNode
	services := []string{"datanode", "namenode"}

	for _, svc := range services {
		if err := h.procMgr.Stop(svc); err != nil {
			util.Warn("Failed to stop %s: %v", svc, err)
		} else {
			pid, _ := h.procMgr.Status(svc)
			if pid == 0 {
				util.Log("Stopped HDFS %s.", svc)
			}
		}

		// Also try to find and stop via process discovery
		var findPID func() (int, error)
		if svc == "namenode" {
			findPID = FindNameNodePID
		} else {
			findPID = FindDataNodePID
		}

		if pid, _ := findPID(); pid != 0 && IsProcessRunning(pid) {
			proc, err := os.FindProcess(pid)
			if err == nil {
				proc.Kill()
				util.Log("Stopped HDFS %s (pid %d).", svc, pid)
			}
		}
	}

	return nil
}

// Status returns the status of HDFS services
func (h *HDFSService) Status() ([]service.ServiceStatus, error) {
	var statuses []service.ServiceStatus

	// Check NameNode
	nnPid, _ := h.procMgr.Status("namenode")
	if nnPid == 0 {
		nnPid, _ = FindNameNodePID()
	}

	statuses = append(statuses, service.ServiceStatus{
		Name:    "namenode",
		Running: nnPid != 0,
		PID:     nnPid,
	})

	// Check DataNode
	dnPid, _ := h.procMgr.Status("datanode")
	if dnPid == 0 {
		dnPid, _ = FindDataNodePID()
	}

	statuses = append(statuses, service.ServiceStatus{
		Name:    "datanode",
		Running: dnPid != 0,
		PID:     dnPid,
	})

	return statuses, nil
}

// Logs tails the HDFS logs
func (h *HDFSService) Logs() error {
	hdfsPaths := h.paths.HDFSPaths()

	logFiles := []string{
		filepath.Join(hdfsPaths.LogsDir, "namenode.log"),
		filepath.Join(hdfsPaths.LogsDir, "datanode.log"),
	}

	// Check which logs exist
	var existingLogs []string
	for _, logFile := range logFiles {
		if util.FileExists(logFile) {
			existingLogs = append(existingLogs, logFile)
		}
	}

	if len(existingLogs) == 0 {
		return fmt.Errorf("no HDFS log files found in %s", hdfsPaths.LogsDir)
	}

	// Tail the logs
	args := append([]string{"-n", "120"}, existingLogs...)
	cmd := exec.Command("tail", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}
