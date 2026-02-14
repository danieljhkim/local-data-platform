package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewPaths(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"

	paths := NewPaths(repoRoot, baseDir)

	if paths.RepoRoot != repoRoot {
		t.Errorf("RepoRoot = %v, want %v", paths.RepoRoot, repoRoot)
	}

	if paths.BaseDir != baseDir {
		t.Errorf("BaseDir = %v, want %v", paths.BaseDir, baseDir)
	}
}

func TestPaths_ProfilesDir(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(repoRoot, "conf", "profiles")
	result := paths.ProfilesDir()

	if result != expected {
		t.Errorf("ProfilesDir() = %v, want %v", result, expected)
	}
}

func TestPaths_CurrentConfDir(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(baseDir, "conf", "current")
	result := paths.CurrentConfDir()

	if result != expected {
		t.Errorf("CurrentConfDir() = %v, want %v", result, expected)
	}
}

func TestPaths_StateDir(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(baseDir, "state")
	result := paths.StateDir()

	if result != expected {
		t.Errorf("StateDir() = %v, want %v", result, expected)
	}
}

func TestPaths_SettingsDir(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(baseDir, "settings")
	result := paths.SettingsDir()

	if result != expected {
		t.Errorf("SettingsDir() = %v, want %v", result, expected)
	}
}

func TestPaths_SettingsFile(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(baseDir, "settings", "setting.json")
	result := paths.SettingsFile()

	if result != expected {
		t.Errorf("SettingsFile() = %v, want %v", result, expected)
	}
}

func TestPaths_ActiveProfileFile(t *testing.T) {
	repoRoot := "/test/repo"
	baseDir := "/test/base"
	paths := NewPaths(repoRoot, baseDir)

	expected := filepath.Join(baseDir, "conf", "active_profile")
	result := paths.ActiveProfileFile()

	if result != expected {
		t.Errorf("ActiveProfileFile() = %v, want %v", result, expected)
	}
}

func TestDefaultBaseDir(t *testing.T) {
	oldBaseDir := os.Getenv("BASE_DIR")
	defer func() {
		if oldBaseDir != "" {
			os.Setenv("BASE_DIR", oldBaseDir)
		} else {
			os.Unsetenv("BASE_DIR")
		}
	}()

	t.Run("ignores BASE_DIR env", func(t *testing.T) {
		testDir := "/custom/base/dir"
		os.Setenv("BASE_DIR", testDir)

		result := DefaultBaseDir()
		homeDir, _ := os.UserHomeDir()
		expected := filepath.Join(homeDir, "local-data-platform")
		if result != expected {
			t.Errorf("DefaultBaseDir() = %v, want %v", result, expected)
		}
	})

	t.Run("without BASE_DIR env", func(t *testing.T) {
		os.Unsetenv("BASE_DIR")

		result := DefaultBaseDir()
		homeDir, _ := os.UserHomeDir()
		expected := filepath.Join(homeDir, "local-data-platform")

		if result != expected {
			t.Errorf("DefaultBaseDir() = %v, want %v", result, expected)
		}
	})
}
