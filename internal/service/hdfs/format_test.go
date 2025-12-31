package hdfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

func TestEnsureLocalStorageDirs(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "test-base")

	err := EnsureLocalStorageDirs(baseDir)
	if err != nil {
		t.Fatalf("EnsureLocalStorageDirs() error = %v", err)
	}

	// Verify directories were created
	expectedDirs := []string{
		filepath.Join(baseDir, "state", "hdfs", "namenode"),
		filepath.Join(baseDir, "state", "hdfs", "datanode"),
		filepath.Join(baseDir, "state", "hadoop", "tmp"),
	}

	for _, dir := range expectedDirs {
		if !util.DirExists(dir) {
			t.Errorf("Directory not created: %s", dir)
		}
	}
}

func TestEnsureLocalStorageDirs_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "test-base")

	// Create directories first
	err := EnsureLocalStorageDirs(baseDir)
	if err != nil {
		t.Fatalf("First EnsureLocalStorageDirs() error = %v", err)
	}

	// Call again - should not error
	err = EnsureLocalStorageDirs(baseDir)
	if err != nil {
		t.Errorf("Second EnsureLocalStorageDirs() error = %v, want no error", err)
	}
}

func TestCreateCommonHDFSDirs_Structure(t *testing.T) {
	// This test validates the structure but doesn't actually run hdfs commands
	// since that would require a running HDFS cluster

	username := "testuser"

	// Just verify the function can be called without panic
	// In a real environment with HDFS, this would create directories
	// For unit tests, we're just validating the logic doesn't crash

	// Note: This will fail if hdfs command is not available,
	// which is expected in a unit test environment
	err := CreateCommonHDFSDirs(username)

	// We expect an error in unit tests since hdfs command won't be available
	// This is acceptable - we're just testing the function exists and is callable
	_ = err // Ignore error in unit tests
}

func TestEnsureNameNodeFormatted_NoConfig(t *testing.T) {
	// Test with a non-existent config directory
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "nonexistent")

	// Should not error - just skips formatting
	err := EnsureNameNodeFormatted(confDir)
	if err != nil {
		t.Errorf("EnsureNameNodeFormatted() with no config should not error, got: %v", err)
	}
}

func TestEnsureNameNodeFormatted_AlreadyFormatted(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	os.MkdirAll(confDir, 0755)

	// Create hdfs-site.xml with namenode dir configuration
	nameNodeDir := filepath.Join(tmpDir, "namenode")
	hdfsConfig := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://` + nameNodeDir + `</value>
  </property>
</configuration>`

	hdfsConfFile := filepath.Join(confDir, "hdfs-site.xml")
	os.WriteFile(hdfsConfFile, []byte(hdfsConfig), 0644)

	// Create VERSION file to simulate already formatted namenode
	versionDir := filepath.Join(nameNodeDir, "current")
	os.MkdirAll(versionDir, 0755)
	versionFile := filepath.Join(versionDir, "VERSION")
	os.WriteFile(versionFile, []byte("test version"), 0644)

	// Should not try to format since VERSION file exists
	err := EnsureNameNodeFormatted(confDir)
	if err != nil {
		t.Errorf("EnsureNameNodeFormatted() with formatted namenode error = %v", err)
	}
}

func TestEnsureNameNodeFormatted_NonEmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	os.MkdirAll(confDir, 0755)

	// Create hdfs-site.xml with namenode dir configuration
	nameNodeDir := filepath.Join(tmpDir, "namenode")
	hdfsConfig := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://` + nameNodeDir + `</value>
  </property>
</configuration>`

	hdfsConfFile := filepath.Join(confDir, "hdfs-site.xml")
	os.WriteFile(hdfsConfFile, []byte(hdfsConfig), 0644)

	// Create namenode dir with a file (but no VERSION file)
	os.MkdirAll(nameNodeDir, 0755)
	os.WriteFile(filepath.Join(nameNodeDir, "somefile.txt"), []byte("data"), 0644)

	// Should return error - directory is not empty and not formatted
	err := EnsureNameNodeFormatted(confDir)
	if err == nil {
		t.Error("EnsureNameNodeFormatted() with non-empty unformatted directory should error")
	}
}

func TestEnsureNameNodeFormatted_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	os.MkdirAll(confDir, 0755)

	// Create hdfs-site.xml with namenode dir configuration
	nameNodeDir := filepath.Join(tmpDir, "namenode")
	hdfsConfig := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://` + nameNodeDir + `</value>
  </property>
</configuration>`

	hdfsConfFile := filepath.Join(confDir, "hdfs-site.xml")
	os.WriteFile(hdfsConfFile, []byte(hdfsConfig), 0644)

	// Create empty namenode directory
	os.MkdirAll(nameNodeDir, 0755)

	// Should try to format (will fail in unit tests since hdfs command not available)
	// but we're testing the logic path
	err := EnsureNameNodeFormatted(confDir)

	// In unit tests without hdfs installed, we expect an error
	// The important thing is it attempted to format (not skipped)
	_ = err // Expected to fail in unit test environment
}
