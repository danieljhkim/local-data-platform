package hive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service"
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

// fakeEnvironment returns a deterministic environment for the given paths,
// mirroring what env.Compute would produce without depending on Hadoop/Hive
// discovery (brew, HIVE_HOME, etc.) being available on the host running
// the tests.
func fakeEnvironment(paths *config.Paths) *env.Environment {
	return &env.Environment{
		BaseDir:     paths.BaseDir,
		RepoRoot:    paths.RepoRoot,
		HiveHome:    filepath.Join(paths.BaseDir, "fake-hive-home"),
		HiveConfDir: paths.CurrentHiveConf(),
	}
}

// newTestHiveService constructs a HiveService using a deterministic fake
// environment, bypassing env.Compute's Hadoop/Hive/Java discovery so tests
// remain hermetic on hosts without those installed.
func newTestHiveService(t *testing.T, paths *config.Paths) *HiveService {
	t.Helper()

	service, err := newHiveServiceWithEnv(paths, fakeEnvironment(paths))
	if err != nil {
		t.Fatalf("newHiveServiceWithEnv() error = %v", err)
	}
	return service
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

	service := newTestHiveService(t, paths)

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

	service := newTestHiveService(t, paths)

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

	service := newTestHiveService(t, paths)

	// Stop when not running should not error
	err := service.Stop()

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

	service := newTestHiveService(t, paths)

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

	service := newTestHiveService(t, paths)

	// Should not error when Postgres is not configured
	err := service.ensurePostgresJDBC()
	if err != nil {
		t.Errorf("ensurePostgresJDBC() should not error when Postgres not configured, got: %v", err)
	}
}

func TestHiveStart_MetastoreMustBeReadyBeforeHiveServer2(t *testing.T) {
	var events []string
	h := &HiveService{
		startMetastoreHook: func(context.Context) (bool, error) {
			events = append(events, "start metastore")
			return true, nil
		},
		waitMetastoreHook: func(context.Context) error {
			events = append(events, "ready metastore")
			return nil
		},
		startHiveServer2Hook: func(context.Context) (bool, error) {
			events = append(events, "start hiveserver2")
			return true, nil
		},
		waitHiveServer2Hook: func(context.Context) error {
			events = append(events, "ready hiveserver2")
			return nil
		},
	}

	result, err := h.startComponents(context.Background())
	if err != nil {
		t.Fatalf("startComponents() error = %v", err)
	}
	wantEvents := []string{"start metastore", "ready metastore", "start hiveserver2", "ready hiveserver2"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if want := []string{"metastore", "hiveserver2"}; !reflect.DeepEqual(result.Started, want) {
		t.Fatalf("result.Started = %#v, want %#v", result.Started, want)
	}
}

func TestHiveStart_FaultInjectionRollsBackOnlyNewComponents(t *testing.T) {
	tests := []struct {
		name              string
		metastoreStarted  bool
		metastoreStartErr error
		serverStarted     bool
		metastoreReady    error
		serverStartErr    error
		serverReady       error
		wantStopped       []string
		wantServerStart   bool
	}{
		{name: "metastore startup failure", metastoreStartErr: errors.New("injected metastore failure")},
		{name: "metastore readiness timeout", metastoreStarted: true, metastoreReady: errors.New("metastore listener timeout"), wantStopped: []string{"metastore"}},
		{name: "HiveServer2 startup failure after new metastore", metastoreStarted: true, serverStartErr: errors.New("injected HiveServer2 failure"), wantStopped: []string{"metastore"}, wantServerStart: true},
		{name: "HiveServer2 startup failure preserves existing metastore", serverStartErr: errors.New("injected HiveServer2 failure"), wantServerStart: true},
		{name: "HiveServer2 readiness timeout", metastoreStarted: true, serverStarted: true, serverReady: errors.New("HiveServer2 listener timeout"), wantStopped: []string{"hiveserver2", "metastore"}, wantServerStart: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stopped []string
			serverStarted := false
			h := &HiveService{
				startMetastoreHook: func(context.Context) (bool, error) { return tc.metastoreStarted, tc.metastoreStartErr },
				waitMetastoreHook:  func(context.Context) error { return tc.metastoreReady },
				startHiveServer2Hook: func(context.Context) (bool, error) {
					serverStarted = true
					return tc.serverStarted, tc.serverStartErr
				},
				waitHiveServer2Hook: func(context.Context) error { return tc.serverReady },
				stopHook: func(name string) error {
					stopped = append(stopped, name)
					return nil
				},
			}

			_, err := h.startComponents(context.Background())
			if err == nil {
				t.Fatal("startComponents() unexpectedly succeeded")
			}
			if serverStarted != tc.wantServerStart {
				t.Fatalf("HiveServer2 dispatched = %v, want %v", serverStarted, tc.wantServerStart)
			}
			if !reflect.DeepEqual(stopped, tc.wantStopped) {
				t.Fatalf("stopped = %#v, want %#v", stopped, tc.wantStopped)
			}
			if !strings.Contains(err.Error(), "listener timeout") && !strings.Contains(err.Error(), "injected HiveServer2 failure") && !strings.Contains(err.Error(), "injected metastore failure") {
				t.Fatalf("startComponents() error lacks failure context: %v", err)
			}
		})
	}
}

func TestHiveWaitForListener_TimeoutIsActionable(t *testing.T) {
	h := &HiveService{
		procMgr:          &service.ProcessManager{LogDir: "/diagnostic/logs"},
		listenerRetries:  1,
		verifyDaemonHook: func(string, string) error { return nil },
		listenerProbeHook: func(context.Context, string) error {
			return errors.New("connection refused")
		},
	}

	err := h.waitForListener(context.Background(), "Hive metastore", "metastore", 19083)
	if err == nil || !strings.Contains(err.Error(), "Hive metastore listener on port 19083") || !strings.Contains(err.Error(), "/diagnostic/logs/metastore.log") {
		t.Fatalf("waitForListener() error = %v, want port and diagnostic log context", err)
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
