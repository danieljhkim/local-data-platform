package hive

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

// setupTestProfile creates a minimal test profile structure using ProfileManager.Init()
func setupTestProfile(tmpDir string) error {
	repoRoot := filepath.Join(tmpDir, "repo")
	baseDir := filepath.Join(tmpDir, "base")
	paths := config.NewPaths(repoRoot, baseDir)
	pm := config.NewProfileManager(paths)
	return pm.Init(false, nil)
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
