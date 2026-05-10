package hive

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// setupTestProfile creates a minimal test profile structure using ProfileManager.Init()
func setupTestProfile(tmpDir string) error {
	repoRoot := filepath.Join(tmpDir, "repo")
	baseDir := filepath.Join(tmpDir, "base")
	paths := config.NewPaths(repoRoot, baseDir)
	pm := config.NewProfileManager(paths)
	return pm.Init(false, nil)
}

func newTestHiveServiceWithConfig(t *testing.T, props ...util.HadoopProperty) *HiveService {
	t.Helper()

	tmpDir := t.TempDir()
	hiveConfDir := filepath.Join(tmpDir, "conf", "current", "hive")
	if err := writeTestHiveSite(hiveConfDir, props...); err != nil {
		t.Fatalf("Failed to write test hive-site.xml: %v", err)
	}

	return &HiveService{
		env: &env.Environment{HiveConfDir: hiveConfDir},
	}
}

func writeTestHiveSite(hiveConfDir string, props ...util.HadoopProperty) error {
	if err := os.MkdirAll(hiveConfDir, 0755); err != nil {
		return err
	}

	cfg := &util.HadoopConfiguration{Properties: props}
	return cfg.WriteXML(filepath.Join(hiveConfDir, "hive-site.xml"))
}

func assertListenerPort(t *testing.T, statuses []ListenerStatus, label string, want int) {
	t.Helper()

	for _, status := range statuses {
		if status.Label == label {
			if status.Port != want {
				t.Fatalf("%s port = %d, want %d", label, status.Port, want)
			}
			return
		}
	}

	t.Fatalf("listener status for %s not found: %#v", label, statuses)
}

func TestHiveService_ListenerStatuses_DefaultPorts(t *testing.T) {
	service := newTestHiveServiceWithConfig(t)

	statuses := service.ListenerStatuses()

	assertListenerPort(t, statuses, "metastore", 9083)
	assertListenerPort(t, statuses, "hiveserver2", 10000)
}

func TestHiveService_ListenerStatuses_CustomMetastorePort(t *testing.T) {
	service := newTestHiveServiceWithConfig(t,
		util.HadoopProperty{Name: "hive.metastore.uris", Value: "thrift://localhost:19083"},
	)

	statuses := service.ListenerStatuses()

	assertListenerPort(t, statuses, "metastore", 19083)
	assertListenerPort(t, statuses, "hiveserver2", 10000)
}

func TestHiveService_ListenerStatuses_CustomHiveServer2Port(t *testing.T) {
	service := newTestHiveServiceWithConfig(t,
		util.HadoopProperty{Name: "hive.server2.thrift.port", Value: "11000"},
	)

	statuses := service.ListenerStatuses()

	assertListenerPort(t, statuses, "metastore", 9083)
	assertListenerPort(t, statuses, "hiveserver2", 11000)
}

func TestHiveService_ListenerStatuses_MalformedPortsFallback(t *testing.T) {
	service := newTestHiveServiceWithConfig(t,
		util.HadoopProperty{Name: "hive.metastore.uris", Value: "not-a-uri"},
		util.HadoopProperty{Name: "hive.server2.thrift.port", Value: "definitely-not-a-port"},
	)

	statuses := service.ListenerStatuses()

	assertListenerPort(t, statuses, "metastore", 9083)
	assertListenerPort(t, statuses, "hiveserver2", 10000)
}

func TestForceStopListenerPorts_ReadsConfiguredHiveSiteFromPidDir(t *testing.T) {
	tmpDir := t.TempDir()
	hiveConfDir := filepath.Join(tmpDir, "conf", "current", "hive")
	if err := writeTestHiveSite(hiveConfDir,
		util.HadoopProperty{Name: "hive.metastore.uris", Value: "thrift://localhost:19083"},
		util.HadoopProperty{Name: "hive.server2.thrift.port", Value: "11000"},
	); err != nil {
		t.Fatalf("Failed to write test hive-site.xml: %v", err)
	}

	pidDir := filepath.Join(tmpDir, "state", "hive", "pids")

	ports := forceStopListenerPorts(pidDir)

	if len(ports) != 2 || ports[0] != 19083 || ports[1] != 11000 {
		t.Fatalf("forceStopListenerPorts() = %#v, want []int{19083, 11000}", ports)
	}
}

func TestNewHiveService(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service, err := NewHiveService(paths)

	if err != nil {
		t.Fatalf("NewHiveService() error = %v", err)
	}

	if service == nil {
		t.Fatal("NewHiveService() returned nil")
	}
}

func TestNewHiveService_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service, err := NewHiveService(paths)
	if err != nil {
		t.Fatalf("NewHiveService() error = %v", err)
	}

	// Verify directories were created
	expectedDirs := []string{
		filepath.Join(baseDir, "state", "hive", "pids"),
		filepath.Join(baseDir, "state", "hive", "logs"),
		filepath.Join(baseDir, "state", "hive", "warehouse"),
	}

	for _, dir := range expectedDirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Directory not created: %s", dir)
		}
	}

	// Verify procMgr is initialized
	if service.procMgr == nil {
		t.Error("ProcessManager not initialized")
	}
}

func TestHiveService_Stop_NotRunning(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service, err := NewHiveService(paths)
	if err != nil {
		t.Fatalf("NewHiveService() error = %v", err)
	}

	// Stop when not running should not error
	err = service.Stop()

	if err != nil {
		t.Errorf("Stop() when not running should not error, got: %v", err)
	}
}

func TestHiveService_Status_NotRunning(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service, err := NewHiveService(paths)
	if err != nil {
		t.Fatalf("NewHiveService() error = %v", err)
	}

	statuses, err := service.Status()

	if err != nil {
		t.Errorf("Status() error = %v", err)
	}

	// Should return status for both services
	if len(statuses) != 2 {
		t.Errorf("Status() returned %d statuses, want 2", len(statuses))
	}

	// Both should be not running
	for _, status := range statuses {
		if status.Running {
			t.Errorf("Service %s should not be running in test", status.Name)
		}
	}
}

func TestHiveService_EnsurePostgresJDBC_NotNeeded(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile (without Postgres in config)
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service, err := NewHiveService(paths)
	if err != nil {
		t.Fatalf("NewHiveService() error = %v", err)
	}

	// Should not error when Postgres is not configured
	err = service.ensurePostgresJDBC()
	if err != nil {
		t.Errorf("ensurePostgresJDBC() should not error when Postgres not configured, got: %v", err)
	}
}

// Note: Full Hive lifecycle tests (Start with actual processes)
// should be done in integration tests where we can start actual Hive
// services and verify they work correctly.
//
// Unit tests focus on:
// - Service initialization
// - Directory creation
// - Status checking for non-running services
// - Configuration validation
