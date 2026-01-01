package yarn

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

func TestNewYARNService(t *testing.T) {
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

	service, err := NewYARNService(paths)

	if err != nil {
		t.Fatalf("NewYARNService() error = %v", err)
	}

	if service == nil {
		t.Fatal("NewYARNService() returned nil")
	}
}

func TestNewYARNService_CreatesDirectories(t *testing.T) {
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

	service, err := NewYARNService(paths)
	if err != nil {
		t.Fatalf("NewYARNService() error = %v", err)
	}

	// Verify directories were created
	expectedDirs := []string{
		filepath.Join(baseDir, "state", "yarn", "pids"),
		filepath.Join(baseDir, "state", "yarn", "logs"),
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

func TestYARNService_Stop_NotRunning(t *testing.T) {
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

	service, err := NewYARNService(paths)
	if err != nil {
		t.Fatalf("NewYARNService() error = %v", err)
	}

	// Stop when not running should not error
	err = service.Stop()

	if err != nil {
		t.Errorf("Stop() when not running should not error, got: %v", err)
	}
}

func TestYARNService_Status_NotRunning(t *testing.T) {
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

	service, err := NewYARNService(paths)
	if err != nil {
		t.Fatalf("NewYARNService() error = %v", err)
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

func TestFindWithJPS_NotFound(t *testing.T) {
	// Try to find a process that doesn't exist
	pid := findWithJPS("NonExistentProcess")

	if pid != 0 {
		t.Errorf("findWithJPS() = %d, want 0 for non-existent process", pid)
	}
}

func TestIsProcessRunning_InvalidPID(t *testing.T) {
	// Test with an invalid/non-existent PID
	running := isProcessRunning(999999)

	// Should return false for non-existent process
	if running {
		t.Error("isProcessRunning() should return false for invalid PID")
	}
}

// Note: Full YARN lifecycle tests (Start with actual processes)
// should be done in integration tests where we can start actual YARN
// services and verify they work correctly.
//
// Unit tests focus on:
// - Service initialization
// - Directory creation
// - Status checking for non-running services
// - Process discovery helper functions
