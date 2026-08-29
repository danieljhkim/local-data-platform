package hive

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// HiveService manages the Hive Metastore and HiveServer2 services
type HiveService struct {
	paths   *config.Paths
	env     *env.Environment
	procMgr *service.ProcessManager

	startMetastoreHook   func(context.Context) (bool, error)
	startHiveServer2Hook func(context.Context) (bool, error)
	waitMetastoreHook    func(context.Context) error
	waitHiveServer2Hook  func(context.Context) error
	verifyDaemonHook     func(string, string) error
	listenerProbeHook    func(context.Context, string) error
	listenerRetries      int
	stopHook             func(string) error
}

// NewHiveService creates a new Hive service manager
func NewHiveService(paths *config.Paths) (*HiveService, error) {
	environment, err := env.Compute(paths)
	if err != nil {
		return nil, fmt.Errorf("failed to compute environment: %w", err)
	}

	stateDir := filepath.Join(paths.StateDir(), "hive")
	pidDir := filepath.Join(stateDir, "pids")
	logDir := filepath.Join(stateDir, "logs")
	warehouseDir := filepath.Join(stateDir, "warehouse")

	if err := util.MkdirAll(pidDir, logDir, warehouseDir); err != nil {
		return nil, fmt.Errorf("failed to create Hive directories: %w", err)
	}

	procMgr := &service.ProcessManager{
		PidDir:      pidDir,
		LogDir:      logDir,
		ValidatePID: hivePIDValidator(),
	}

	return &HiveService{
		paths:   paths,
		env:     environment,
		procMgr: procMgr,
	}, nil
}

// Start starts the Hive Metastore and HiveServer2
func (h *HiveService) Start() error {
	_, err := h.StartContext(context.Background())
	return err
}

// StartContext starts Hive transactionally. The metastore listener must be
// ready before HiveServer2 is dispatched, and both listeners are required for
// success.
func (h *HiveService) StartContext(ctx context.Context) (service.StartResult, error) {
	util.Log("Starting Hive services...")

	// Clean up stale Derby lock files if using embedded Derby
	h.cleanStaleDerbyLocks()

	// Ensure required JDBC drivers are available.
	if err := h.ensurePostgresJDBC(); err != nil {
		return service.StartResult{}, err
	}

	// Ensure metastore schema is initialized
	if err := h.ensureMetastoreSchema(); err != nil {
		return service.StartResult{}, err
	}

	return h.startComponents(ctx)
}

func (h *HiveService) startComponents(ctx context.Context) (service.StartResult, error) {
	return service.RunStartSteps(ctx, []service.StartStep{
		{
			Name:  "metastore",
			Start: h.startMetastoreStep,
			Ready: h.waitForMetastoreStep,
			Stop:  func() error { return h.stopStarted("metastore") },
		},
		{
			Name:  "hiveserver2",
			Start: h.startHiveServer2Step,
			Ready: h.waitForHiveServer2Step,
			Stop:  func() error { return h.stopStarted("hiveserver2") },
		},
	})
}

func (h *HiveService) startMetastoreStep(ctx context.Context) (bool, error) {
	if h.startMetastoreHook != nil {
		return h.startMetastoreHook(ctx)
	}
	return h.startMetastore()
}

func (h *HiveService) startHiveServer2Step(ctx context.Context) (bool, error) {
	if h.startHiveServer2Hook != nil {
		return h.startHiveServer2Hook(ctx)
	}
	return h.startHiveServer2()
}

func (h *HiveService) waitForMetastoreStep(ctx context.Context) error {
	if h.waitMetastoreHook != nil {
		return h.waitMetastoreHook(ctx)
	}
	return h.waitForListener(ctx, "Hive metastore", "metastore", h.listenerPorts().Metastore)
}

func (h *HiveService) waitForHiveServer2Step(ctx context.Context) error {
	if h.waitHiveServer2Hook != nil {
		return h.waitHiveServer2Hook(ctx)
	}
	return h.waitForListener(ctx, "HiveServer2", "hiveserver2", h.listenerPorts().HiveServer2)
}

// ensurePostgresJDBC ensures Postgres JDBC driver is available if needed.
func (h *HiveService) ensurePostgresJDBC() error {
	dbType, _, err := h.detectMetastoreConfig()
	if err != nil {
		return nil
	}
	if err := h.ensureJDBCDriver(dbType); err != nil {
		return err
	}

	return nil
}

// startMetastore starts the Hive Metastore
func (h *HiveService) startMetastore() (bool, error) {
	name := "metastore"

	// Check if already running
	pid, err := h.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("Hive metastore already running (pid %d).", pid)
		return false, nil
	}

	// Start the Metastore
	cmd := exec.Command("hive", "--service", "metastore")
	cmd.Env = h.env.Export()

	logFile := name + ".log"
	startedPid, err := h.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return false, fmt.Errorf("failed to start Hive metastore: %w", err)
	}

	util.Success("Hive metastore started (pid %d).", startedPid)
	return true, nil
}

// startHiveServer2 starts the HiveServer2
func (h *HiveService) startHiveServer2() (bool, error) {
	name := "hiveserver2"

	// Check if already running
	pid, err := h.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("HiveServer2 already running (pid %d).", pid)
		return false, nil
	}

	// Start HiveServer2
	cmd := exec.Command("hive", "--service", "hiveserver2")
	cmd.Env = h.env.Export()

	logFile := name + ".log"
	startedPid, err := h.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return false, fmt.Errorf("failed to start HiveServer2: %w", err)
	}

	util.Success("HiveServer2 started (pid %d).", startedPid)
	return true, nil
}

func (h *HiveService) verifyDaemon(name, label string) error {
	if h.verifyDaemonHook != nil {
		return h.verifyDaemonHook(name, label)
	}
	pid, err := h.procMgr.Status(name)
	if err != nil {
		return fmt.Errorf("could not verify %s process: %w", label, err)
	}
	if pid == 0 {
		return fmt.Errorf("%s process exited before becoming ready (check logs: %s)", label, filepath.Join(h.procMgr.LogDir, name+".log"))
	}
	if h.procMgr.ValidatePID != nil {
		if err := h.procMgr.ValidatePID(name, pid); err != nil {
			return fmt.Errorf("%s pid %d failed ownership validation: %w", label, pid, err)
		}
	}
	return nil
}

func (h *HiveService) stopStarted(name string) error {
	if h.stopHook != nil {
		return h.stopHook(name)
	}
	return h.procMgr.Stop(name)
}

// Rollback stops only daemons recorded as newly started by StartContext.
func (h *HiveService) Rollback(result service.StartResult) error {
	return service.RollbackStarted(result, h.stopStarted)
}

// Stop stops the Hive Metastore and HiveServer2
func (h *HiveService) Stop() error {
	util.Log("Stopping Hive services...")

	// Stop in reverse order: HiveServer2, then Metastore
	services := []string{"hiveserver2", "metastore"}

	for _, svc := range services {
		pid, err := h.procMgr.Status(svc)
		if err == nil && pid > 0 {
			if err := h.procMgr.Stop(svc); err != nil {
				util.Warn("Failed to stop Hive %s: %v", svc, err)
			} else {
				util.Success("Stopped Hive %s (pid %d).", svc, pid)
			}
		}

		// Clean up PID file
		pidFile := filepath.Join(h.procMgr.PidDir, svc+".pid")
		removeFile(pidFile)
	}

	return nil
}

// StopForce performs a force-stop of Hive services
func (h *HiveService) StopForce() error {
	ports := h.listenerPorts()
	return ForceStop(h.procMgr.PidDir, ports.slice()...)
}

// ListenerStatus represents the status of a Hive listener port
type ListenerStatus struct {
	Label     string // e.g., "metastore", "hiveserver2"
	Port      int
	Listening bool
	PID       string // PID of the listener process (if listening)
	Cmd       string // Command name (if listening)
}

// Status returns the status of Hive services
func (h *HiveService) Status() ([]service.ServiceStatus, error) {
	services := []string{"metastore", "hiveserver2"}
	statuses := make([]service.ServiceStatus, 0, len(services))

	for _, svc := range services {
		status := service.ServiceStatus{Name: svc}

		pid, err := h.procMgr.Status(svc)
		if err == nil && pid > 0 {
			status.Running = true
			status.PID = pid
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

// ListenerStatuses returns the listener status for Hive ports
func (h *HiveService) ListenerStatuses() []ListenerStatus {
	ports := h.listenerPorts()
	if _, err := exec.LookPath("lsof"); err != nil {
		return []ListenerStatus{
			{Label: "metastore", Port: ports.Metastore},
			{Label: "hiveserver2", Port: ports.HiveServer2},
		}
	}

	return []ListenerStatus{
		h.checkListener(ports.Metastore, "metastore"),
		h.checkListener(ports.HiveServer2, "hiveserver2"),
	}
}

// checkListener checks if a port is listening and returns a ListenerStatus
func (h *HiveService) checkListener(port int, label string) ListenerStatus {
	ls := ListenerStatus{Label: label, Port: port}

	cmd := exec.Command("lsof", "-nP", fmt.Sprintf("-iTCP:%d", port), "-sTCP:LISTEN")
	output, err := cmd.Output()
	if err != nil {
		return ls
	}

	lines := strings.Split(string(output), "\n")
	for i, line := range lines {
		if i == 0 || line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			ls.Listening = true
			ls.PID = fields[1]
			ls.Cmd = fields[0]
			return ls
		}
	}

	return ls
}

// cleanStaleDerbyLocks removes stale Derby lock files if the metastore uses
// embedded Derby and no Hive process currently holds the lock.
func (h *HiveService) cleanStaleDerbyLocks() {
	dbType, dbURL, err := h.detectMetastoreConfig()
	if err != nil || dbType != metastore.Derby {
		return
	}

	// Extract the databaseName path from the Derby JDBC URL
	dbPath := extractDerbyDBPath(dbURL)
	if dbPath == "" {
		return
	}

	lockFile := filepath.Join(dbPath, "db.lck")
	if _, err := os.Stat(lockFile); os.IsNotExist(err) {
		return
	}

	// Check if any Hive process is actually running (metastore or HS2)
	metaPid, _ := h.procMgr.Status("metastore")
	hs2Pid, _ := h.procMgr.Status("hiveserver2")
	if metaPid > 0 || hs2Pid > 0 {
		return // A live process holds the lock
	}

	util.Log("Removing stale Derby lock files from %s", dbPath)
	removeFile(filepath.Join(dbPath, "db.lck"))
	removeFile(filepath.Join(dbPath, "dbex.lck"))
}

// extractDerbyDBPath extracts the databaseName value from a Derby JDBC URL.
// e.g. "jdbc:derby:;databaseName=/path/to/db;create=true" -> "/path/to/db"
func extractDerbyDBPath(dbURL string) string {
	for _, part := range strings.Split(dbURL, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "databaseName=") {
			return strings.TrimPrefix(part, "databaseName=")
		}
	}
	return ""
}

// waitForHiveServer2 polls the HiveServer2 thrift port until it is accepting
// connections or a timeout is reached.
func (h *HiveService) waitForHiveServer2() error {
	return h.waitForHiveServer2Step(context.Background())
}

func (h *HiveService) waitForListener(ctx context.Context, label, processName string, port int) error {
	addr := fmt.Sprintf("localhost:%d", port)
	util.Log("Waiting for %s to be ready on port %d...", label, port)

	maxRetries := h.listenerRetries
	if maxRetries <= 0 {
		maxRetries = 30
	}
	for i := 0; i < maxRetries; i++ {
		if err := h.verifyDaemon(processName, label); err != nil {
			return err
		}
		if err := h.probeListener(ctx, addr); err == nil {
			util.Success("%s is ready.", label)
			return nil
		}
		if i < maxRetries-1 {
			if err := waitForContext(ctx, 2*time.Second); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf("%s listener on port %d did not become ready within 60 seconds (check logs: %s)", label, port, filepath.Join(h.procMgr.LogDir, processName+".log"))
}

func (h *HiveService) probeListener(ctx context.Context, address string) error {
	if h.listenerProbeHook != nil {
		return h.listenerProbeHook(ctx, address)
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("failed to close readiness connection: %w", err)
	}
	return nil
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// getHS2Port reads the HiveServer2 thrift port from the active hive-site.xml.
// Falls back to 10000 if not configured.
func (h *HiveService) getHS2Port() int {
	return h.listenerPorts().HiveServer2
}

// Logs displays Hive service logs
func (h *HiveService) Logs() error {
	logDir := h.procMgr.LogDir

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return fmt.Errorf("no Hive logs directory found: %s (have you started Hive?)", logDir)
	}

	logFiles := []string{
		filepath.Join(logDir, "metastore.log"),
		filepath.Join(logDir, "hiveserver2.log"),
	}

	for _, logFile := range logFiles {
		fmt.Printf("==> %s\n", logFile)
		if _, err := os.Stat(logFile); err == nil {
			cmd := exec.Command("tail", "-n", "120", logFile)
			cmd.Stdout = os.Stdout
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to tail Hive log %s: %w", logFile, err)
			}
		} else {
			fmt.Println("(missing)")
		}
		fmt.Println()
	}

	return nil
}
