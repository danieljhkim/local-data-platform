package hdfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"

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

	startNameNodeHook func(context.Context, []string) (bool, error)
	startDataNodeHook func(context.Context, []string) (bool, error)
	waitSafeModeHook  func(context.Context, int, []string) error
	verifyDaemonHook  func(string) error
	createDirsHook    func([]string) error
	stopHook          func(string) error
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
	procMgr.ValidatePID = hdfsPIDValidator()

	return &HDFSService{
		paths:   paths,
		env:     environment,
		procMgr: procMgr,
	}, nil
}

// Start starts the HDFS NameNode and DataNode
// Mirrors ld_hdfs_start
func (h *HDFSService) Start() error {
	_, err := h.StartContext(context.Background())
	return err
}

// StartContext starts HDFS transactionally and returns only the daemons
// created by this invocation.
func (h *HDFSService) StartContext(ctx context.Context) (service.StartResult, error) {
	// Ensure Hadoop is available
	if h.env.HadoopHome == "" {
		return service.StartResult{}, fmt.Errorf("hadoop not found (HADOOP_HOME not set). Install with: brew install hadoop")
	}

	// Ensure local storage directories exist
	if err := EnsureLocalStorageDirs(h.paths.BaseDir); err != nil {
		return service.StartResult{}, err
	}

	runtimeEnv := h.env.MergeWithCurrent()

	// Ensure NameNode is formatted with the same active overlay as all
	// subsequent daemon and control commands.
	if err := EnsureNameNodeFormattedWithEnv(h.env.HadoopConfDir, runtimeEnv); err != nil {
		return service.StartResult{}, err
	}

	// Ensure log and PID directories exist
	hdfsPaths := h.paths.HDFSPaths()
	if err := util.MkdirAll(hdfsPaths.LogsDir, hdfsPaths.PidsDir); err != nil {
		return service.StartResult{}, err
	}

	return h.startComponents(ctx, runtimeEnv, hdfsPaths)
}

func (h *HDFSService) startComponents(ctx context.Context, runtimeEnv []string, hdfsPaths *config.ServicePaths) (service.StartResult, error) {
	return service.RunStartSteps(ctx, []service.StartStep{
		{
			Name:  "namenode",
			Start: func(ctx context.Context) (bool, error) { return h.startNameNodeStep(ctx, runtimeEnv) },
			Ready: func(context.Context) error { return h.verifyDaemonStep("namenode") },
			Stop:  func() error { return h.stopStarted("namenode") },
		},
		{
			Name:  "datanode",
			Start: func(ctx context.Context) (bool, error) { return h.startDataNodeStep(ctx, runtimeEnv) },
			Ready: func(ctx context.Context) error {
				if err := h.verifyDaemonStep("datanode"); err != nil {
					return err
				}
				util.Log("Waiting for NameNode to exit safe mode...")
				wait := h.waitSafeModeHook
				if wait == nil {
					wait = WaitForSafeModeWithContext
				}
				if err := wait(ctx, 10, runtimeEnv); err != nil {
					return fmt.Errorf("%w (check logs: %s)", err, hdfsPaths.LogsDir)
				}
				if h.createDirsHook != nil {
					return h.createDirsHook(runtimeEnv)
				}
				return h.createCommonDirectories(runtimeEnv)
			},
			Stop: func() error { return h.stopStarted("datanode") },
		},
	})
}

func (h *HDFSService) verifyDaemonStep(name string) error {
	if h.verifyDaemonHook != nil {
		return h.verifyDaemonHook(name)
	}
	return h.verifyDaemon(name)
}

func (h *HDFSService) startNameNodeStep(ctx context.Context, runtimeEnv []string) (bool, error) {
	if h.startNameNodeHook != nil {
		return h.startNameNodeHook(ctx, runtimeEnv)
	}
	return h.startNameNode(runtimeEnv)
}

func (h *HDFSService) startDataNodeStep(ctx context.Context, runtimeEnv []string) (bool, error) {
	if h.startDataNodeHook != nil {
		return h.startDataNodeHook(ctx, runtimeEnv)
	}
	return h.startDataNode(runtimeEnv)
}

func (h *HDFSService) createCommonDirectories(runtimeEnv []string) error {
	util.Log("Creating common HDFS directories...")
	currentUser, err := user.Current()
	username := "hadoop"
	if err == nil {
		username = currentUser.Username
	}
	if err := CreateCommonHDFSDirsWithEnv(username, runtimeEnv); err != nil {
		return fmt.Errorf("failed to create common HDFS directories: %w", err)
	}
	return nil
}

// startNameNode starts the NameNode process
func (h *HDFSService) startNameNode(runtimeEnv []string) (bool, error) {
	// Check if already running
	pid, _ := h.procMgr.Status("namenode")
	foundByPIDFile := pid != 0
	if pid == 0 {
		// Try to find via jps/pgrep
		pid, _ = FindNameNodePID()
	}

	// If running, check if using current config
	if pid != 0 {
		if !CheckConfOverlay(pid, h.env.HadoopConfDir) {
			util.Log("HDFS NameNode running but not using current overlay config; restarting (pid %d).", pid)
			if err := h.stopStaleDaemon("namenode", pid, foundByPIDFile); err != nil {
				return false, err
			}
			pid = 0
		}
	}

	// If still running, we're done
	if pid != 0 && IsProcessRunning(pid) {
		// Update PID file
		hdfsPaths := h.paths.HDFSPaths()
		pidFile := filepath.Join(hdfsPaths.PidsDir, "namenode.pid")
		if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			util.Warn("Failed to update NameNode PID file: %v", err)
		}
		util.Log("HDFS NameNode already running (pid %d).", pid)
		return false, nil
	}

	// Start NameNode
	cmd := exec.Command("hdfs", "namenode")
	cmd.Env = runtimeEnv

	pid, err := h.procMgr.Start("namenode", cmd, "namenode.log")
	if err != nil {
		return false, fmt.Errorf("failed to start NameNode: %w", err)
	}

	util.Success("HDFS NameNode started (pid %d).", pid)
	return true, nil
}

// startDataNode starts the DataNode process
func (h *HDFSService) startDataNode(runtimeEnv []string) (bool, error) {
	// Check if already running
	pid, _ := h.procMgr.Status("datanode")
	foundByPIDFile := pid != 0
	if pid == 0 {
		// Try to find via jps/pgrep
		pid, _ = FindDataNodePID()
	}

	// If running, check if using current config
	if pid != 0 {
		if !CheckConfOverlay(pid, h.env.HadoopConfDir) {
			util.Log("HDFS DataNode running but not using current overlay config; restarting (pid %d).", pid)
			if err := h.stopStaleDaemon("datanode", pid, foundByPIDFile); err != nil {
				return false, err
			}
			pid = 0
		}
	}

	// If still running, we're done
	if pid != 0 && IsProcessRunning(pid) {
		// Update PID file
		hdfsPaths := h.paths.HDFSPaths()
		pidFile := filepath.Join(hdfsPaths.PidsDir, "datanode.pid")
		if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
			util.Warn("Failed to update DataNode PID file: %v", err)
		}
		util.Log("HDFS DataNode already running (pid %d).", pid)
		return false, nil
	}

	// Start DataNode
	cmd := exec.Command("hdfs", "datanode")
	cmd.Env = runtimeEnv

	pid, err := h.procMgr.Start("datanode", cmd, "datanode.log")
	if err != nil {
		return false, fmt.Errorf("failed to start DataNode: %w", err)
	}

	util.Success("HDFS DataNode started (pid %d).", pid)
	return true, nil
}

func (h *HDFSService) verifyDaemon(name string) error {
	pid, err := h.procMgr.Status(name)
	if err != nil {
		return fmt.Errorf("could not verify HDFS %s process: %w", name, err)
	}
	if pid == 0 {
		return fmt.Errorf("HDFS %s process is not alive (check logs: %s)", name, filepath.Join(h.procMgr.LogDir, name+".log"))
	}
	if h.procMgr.ValidatePID != nil {
		if err := h.procMgr.ValidatePID(name, pid); err != nil {
			return fmt.Errorf("HDFS %s pid %d failed ownership validation: %w", name, pid, err)
		}
	}
	return nil
}

func (h *HDFSService) stopStarted(name string) error {
	if h.stopHook != nil {
		return h.stopHook(name)
	}
	return h.procMgr.Stop(name)
}

// Rollback stops only daemons recorded as newly started by StartContext.
func (h *HDFSService) Rollback(result service.StartResult) error {
	return service.RollbackStarted(result, h.stopStarted)
}

func (h *HDFSService) stopStaleDaemon(name string, pid int, foundByPIDFile bool) error {
	if foundByPIDFile {
		if err := h.procMgr.Stop(name); err != nil {
			return fmt.Errorf("failed to stop stale HDFS %s via PID file: %w", name, err)
		}
		if err := terminateHDFSPID(pid); err != nil {
			return fmt.Errorf("failed to stop stale HDFS %s pid %d after PID-file SIGTERM: %w", name, pid, err)
		}
		return nil
	}

	if err := terminateHDFSPID(pid); err != nil {
		return fmt.Errorf("failed to stop discovered HDFS %s pid %d before restart: %w", name, pid, err)
	}
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
				util.Success("Stopped HDFS %s.", svc)
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
			if err := terminateHDFSPID(pid); err != nil {
				util.Warn("Failed to stop HDFS %s (pid %d): %v", svc, pid, err)
			} else {
				util.Success("Stopped HDFS %s (pid %d).", svc, pid)
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
