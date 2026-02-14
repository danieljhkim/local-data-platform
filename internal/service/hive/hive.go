package hive

import (
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
	paths                 *config.Paths
	env                   *env.Environment
	procMgr               *service.ProcessManager
	usesPostgresMetastore bool
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
		PidDir: pidDir,
		LogDir: logDir,
	}

	return &HiveService{
		paths:   paths,
		env:     environment,
		procMgr: procMgr,
	}, nil
}

// Start starts the Hive Metastore and HiveServer2
func (h *HiveService) Start() error {
	util.Log("Starting Hive services...")

	// Clean up stale Derby lock files if using embedded Derby
	h.cleanStaleDerbyLocks()

	// Ensure required JDBC drivers are available.
	if err := h.ensurePostgresJDBC(); err != nil {
		return err
	}

	// Ensure metastore schema is initialized
	if err := h.ensureMetastoreSchema(); err != nil {
		return err
	}

	// Start Metastore
	if err := h.startMetastore(); err != nil {
		return err
	}

	// Start HiveServer2
	if err := h.startHiveServer2(); err != nil {
		return err
	}

	// Wait for HiveServer2 to be ready for connections
	if err := h.waitForHiveServer2(); err != nil {
		util.Warn("HiveServer2 may not be ready yet: %v", err)
	}

	return nil
}

// ensurePostgresJDBC ensures Postgres JDBC driver is available if needed
// Also sets h.usesPostgresMetastore if Postgres is detected
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
func (h *HiveService) startMetastore() error {
	name := "metastore"

	// Check if already running
	pid, err := h.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("Hive metastore already running (pid %d).", pid)
		return nil
	}

	// Start the Metastore
	cmd := exec.Command("hive", "--service", "metastore")
	cmd.Env = h.env.Export()

	logFile := name + ".log"
	startedPid, err := h.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return fmt.Errorf("failed to start Hive metastore: %w", err)
	}

	util.Log("Hive metastore started (pid %d).", startedPid)
	return nil
}

// startHiveServer2 starts the HiveServer2
func (h *HiveService) startHiveServer2() error {
	name := "hiveserver2"

	// Check if already running
	pid, err := h.procMgr.Status(name)
	if err == nil && pid > 0 {
		util.Log("HiveServer2 already running (pid %d).", pid)
		return nil
	}

	// Start HiveServer2
	cmd := exec.Command("hive", "--service", "hiveserver2")
	cmd.Env = h.env.Export()

	logFile := name + ".log"
	startedPid, err := h.procMgr.Start(name, cmd, logFile)
	if err != nil {
		return fmt.Errorf("failed to start HiveServer2: %w", err)
	}

	util.Log("HiveServer2 started (pid %d).", startedPid)
	return nil
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
				util.Log("Stopped Hive %s (pid %d).", svc, pid)
			}
		}

		// Clean up PID file
		pidFile := filepath.Join(h.procMgr.PidDir, svc+".pid")
		os.Remove(pidFile)
	}

	return nil
}

// StopForce performs a force-stop of Hive services
func (h *HiveService) StopForce() error {
	return ForceStop(h.procMgr.PidDir)
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

	// Also show listener status
	fmt.Println()
	fmt.Println("listeners:")
	h.showListenerStatus()

	return statuses, nil
}

// showListenerStatus shows the status of Hive listeners
func (h *HiveService) showListenerStatus() {
	if _, err := exec.LookPath("lsof"); err != nil {
		fmt.Println("  WARN: lsof not found; cannot check 9083/10000 listeners")
		return
	}

	h.showListenerLine(9083, "metastore")
	h.showListenerLine(10000, "hiveserver2")
}

// showListenerLine shows listener status for a port
func (h *HiveService) showListenerLine(port int, label string) {
	cmd := exec.Command("lsof", "-nP", fmt.Sprintf("-iTCP:%d", port), "-sTCP:LISTEN")
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("  %s:%d not listening\n", label, port)
		return
	}

	lines := strings.Split(string(output), "\n")
	found := false

	// Skip header line
	for i, line := range lines {
		if i == 0 || line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			cmdName := fields[0]
			pid := fields[1]
			fmt.Printf("  %s:%d listening (pid %s, cmd %s)\n", label, port, pid, cmdName)
			found = true
		}
	}

	if !found {
		fmt.Printf("  %s:%d not listening\n", label, port)
	}
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
	os.Remove(filepath.Join(dbPath, "db.lck"))
	os.Remove(filepath.Join(dbPath, "dbex.lck"))
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
	port := h.getHS2Port()
	addr := fmt.Sprintf("localhost:%d", port)

	util.Log("Waiting for HiveServer2 to be ready on port %d...", port)

	maxRetries := 30 // 30 x 2s = 60s max
	for i := 0; i < maxRetries; i++ {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			util.Log("HiveServer2 is ready.")
			return nil
		}

		// Verify the process is still alive
		pid, _ := h.procMgr.Status("hiveserver2")
		if pid == 0 {
			return fmt.Errorf("HiveServer2 process exited before becoming ready (check logs: %s)",
				filepath.Join(h.procMgr.LogDir, "hiveserver2.log"))
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("HiveServer2 did not become ready within 60 seconds")
}

// getHS2Port reads the HiveServer2 thrift port from the active hive-site.xml.
// Falls back to 10000 if not configured.
func (h *HiveService) getHS2Port() int {
	hiveSite := filepath.Join(h.env.HiveConfDir, "hive-site.xml")
	cfg, err := util.ParseHadoopXML(hiveSite)
	if err != nil {
		return 10000
	}
	portStr := strings.TrimSpace(cfg.GetProperty("hive.server2.thrift.port"))
	if portStr == "" {
		return 10000
	}
	port := 10000
	fmt.Sscanf(portStr, "%d", &port)
	return port
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
			cmd.Run()
		} else {
			fmt.Println("(missing)")
		}
		fmt.Println()
	}

	return nil
}
