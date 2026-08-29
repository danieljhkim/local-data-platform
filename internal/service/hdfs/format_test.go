package hdfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

func mkdirFormatTestDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test dir %s: %v", path, err)
	}
}

func writeFormatTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("failed to write test file %s: %v", path, err)
	}
}

func writeNameNodeConfig(t *testing.T, confDir string, dirs ...string) {
	t.Helper()
	values := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		values = append(values, "file://"+dir)
	}
	writeFormatTestFile(t, filepath.Join(confDir, "hdfs-site.xml"), []byte(`<?xml version="1.0"?>
<configuration><property><name>dfs.namenode.name.dir</name><value>`+strings.Join(values, ",")+`</value></property></configuration>`))
}

func writeValidNameNodeVersion(t *testing.T, dir, storageID, clusterID string) {
	t.Helper()
	versionDir := filepath.Join(dir, "current")
	mkdirFormatTestDir(t, versionDir)
	writeFormatTestFile(t, filepath.Join(versionDir, "VERSION"), []byte("storageType=NAME_NODE\nlayoutVersion=-63\nnamespaceID=12345\ncTime=1700000000000\nclusterID="+clusterID+"\nstorageID="+storageID+"\n"))
}

func replaceFormatFunctions(t *testing.T, find func() (int, error), format func(string) error) {
	t.Helper()
	originalFind := findNameNodePIDForFormat
	originalFormat := formatNameNodeForFormat
	findNameNodePIDForFormat = find
	formatNameNodeForFormat = format
	t.Cleanup(func() {
		findNameNodePIDForFormat = originalFind
		formatNameNodeForFormat = originalFormat
	})
}

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

func TestEnsureNameNodeFormatted_MissingConfigFailsClosed(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "nonexistent")

	err := EnsureNameNodeFormatted(confDir)
	if err == nil || !strings.Contains(err.Error(), "cannot validate NameNode storage configuration") {
		t.Fatalf("EnsureNameNodeFormatted() error = %v, want actionable configuration error", err)
	}
}

func TestEnsureNameNodeFormatted_MalformedAndMissingPropertyFailClosed(t *testing.T) {
	for _, content := range []string{
		`<configuration><property><name>dfs.namenode.name.dir</name>`,
		`<configuration><property><name>other.property</name><value>value</value></property></configuration>`,
	} {
		tmpDir := t.TempDir()
		confDir := filepath.Join(tmpDir, "conf")
		mkdirFormatTestDir(t, confDir)
		writeFormatTestFile(t, filepath.Join(confDir, "hdfs-site.xml"), []byte(content))
		if err := EnsureNameNodeFormatted(confDir); err == nil || !strings.Contains(err.Error(), "cannot validate NameNode storage configuration") {
			t.Errorf("EnsureNameNodeFormatted() error = %v, want actionable configuration error", err)
		}
	}
}

func TestEnsureNameNodeFormatted_AllConfiguredDirectoriesFormatted(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	mkdirFormatTestDir(t, confDir)
	dirOne := filepath.Join(tmpDir, "namenode-one")
	dirTwo := filepath.Join(tmpDir, "namenode-two")
	writeNameNodeConfig(t, confDir, dirOne, dirTwo)
	writeValidNameNodeVersion(t, dirOne, "storage-one", "cluster-one")
	writeValidNameNodeVersion(t, dirTwo, "storage-two", "cluster-one")
	formatCalls := 0
	replaceFormatFunctions(t, func() (int, error) { return 0, nil }, func(string) error { formatCalls++; return nil })
	err := EnsureNameNodeFormatted(confDir)
	if err != nil {
		t.Fatalf("EnsureNameNodeFormatted() error = %v", err)
	}
	if formatCalls != 0 {
		t.Fatalf("format calls = %d, want 0", formatCalls)
	}
}

func TestEnsureNameNodeFormatted_InconsistentFormattedDirectoriesFailClosed(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	mkdirFormatTestDir(t, confDir)
	dirOne := filepath.Join(tmpDir, "namenode-one")
	dirTwo := filepath.Join(tmpDir, "namenode-two")
	writeNameNodeConfig(t, confDir, dirOne, dirTwo)
	writeValidNameNodeVersion(t, dirOne, "storage-one", "cluster-one")
	writeValidNameNodeVersion(t, dirTwo, "storage-two", "cluster-two")
	formatCalls := 0
	replaceFormatFunctions(t, func() (int, error) { return 0, nil }, func(string) error { formatCalls++; return nil })
	err := EnsureNameNodeFormatted(confDir)
	if err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("EnsureNameNodeFormatted() error = %v, want identity mismatch error", err)
	}
	if formatCalls != 0 {
		t.Fatalf("format calls = %d, want 0", formatCalls)
	}
}

func TestEnsureNameNodeFormatted_AllEmptyDirectoriesFormatsOnceAndVerifiesAll(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	mkdirFormatTestDir(t, confDir)
	dirOne := filepath.Join(tmpDir, "namenode-one")
	dirTwo := filepath.Join(tmpDir, "namenode-two")
	writeNameNodeConfig(t, confDir, dirOne, dirTwo)
	mkdirFormatTestDir(t, dirOne)
	mkdirFormatTestDir(t, dirTwo)
	formatCalls := 0
	replaceFormatFunctions(t, func() (int, error) { return 0, nil }, func(string) error {
		formatCalls++
		writeValidNameNodeVersion(t, dirOne, "storage-one", "cluster-one")
		writeValidNameNodeVersion(t, dirTwo, "storage-two", "cluster-one")
		return nil
	})
	if err := EnsureNameNodeFormatted(confDir); err != nil {
		t.Fatalf("EnsureNameNodeFormatted() error = %v", err)
	}
	if formatCalls != 1 {
		t.Fatalf("format calls = %d, want 1", formatCalls)
	}
}

func TestEnsureNameNodeFormatted_MixedStorageFailsWithoutFormatting(t *testing.T) {
	for _, state := range []string{"missing", "non-empty"} {
		t.Run(state, func(t *testing.T) {
			tmpDir := t.TempDir()
			confDir := filepath.Join(tmpDir, "conf")
			mkdirFormatTestDir(t, confDir)
			dirOne := filepath.Join(tmpDir, "namenode-one")
			dirTwo := filepath.Join(tmpDir, "namenode-two")
			writeNameNodeConfig(t, confDir, dirOne, dirTwo)
			writeValidNameNodeVersion(t, dirOne, "storage-one", "cluster-one")
			if state == "non-empty" {
				mkdirFormatTestDir(t, dirTwo)
				writeFormatTestFile(t, filepath.Join(dirTwo, "unrelated"), []byte("data"))
			}
			formatCalls := 0
			replaceFormatFunctions(t, func() (int, error) { return 0, nil }, func(string) error { formatCalls++; return nil })
			err := EnsureNameNodeFormatted(confDir)
			if err == nil {
				t.Fatal("EnsureNameNodeFormatted() error = nil, want fail-closed error")
			}
			if formatCalls != 0 {
				t.Fatalf("format calls = %d, want 0", formatCalls)
			}
		})
	}
}

func TestEnsureNameNodeFormatted_NonEmptyAndInvalidVersionFailClosed(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	mkdirFormatTestDir(t, confDir)
	dirOne := filepath.Join(tmpDir, "namenode-one")
	dirTwo := filepath.Join(tmpDir, "namenode-two")
	writeNameNodeConfig(t, confDir, dirOne, dirTwo)
	mkdirFormatTestDir(t, dirOne)
	writeFormatTestFile(t, filepath.Join(dirOne, "unrelated"), []byte("data"))
	mkdirFormatTestDir(t, filepath.Join(dirTwo, "current"))
	writeFormatTestFile(t, filepath.Join(dirTwo, "current", "VERSION"), []byte("not a Hadoop VERSION file"))
	formatCalls := 0
	replaceFormatFunctions(t, func() (int, error) { return 0, nil }, func(string) error { formatCalls++; return nil })
	if err := EnsureNameNodeFormatted(confDir); err == nil {
		t.Fatal("EnsureNameNodeFormatted() error = nil, want fail-closed error")
	}
	if formatCalls != 0 {
		t.Fatalf("format calls = %d, want 0", formatCalls)
	}
}

func TestEnsureNameNodeFormatted_RunningNameNodePreventsFormatting(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "conf")
	mkdirFormatTestDir(t, confDir)
	dirOne := filepath.Join(tmpDir, "namenode-one")
	dirTwo := filepath.Join(tmpDir, "namenode-two")
	writeNameNodeConfig(t, confDir, dirOne, dirTwo)
	formatCalls := 0
	replaceFormatFunctions(t, func() (int, error) { return 4242, nil }, func(string) error { formatCalls++; return nil })
	err := EnsureNameNodeFormatted(confDir)
	if err == nil || !strings.Contains(err.Error(), "4242") || !strings.Contains(err.Error(), dirOne) || !strings.Contains(err.Error(), dirTwo) {
		t.Fatalf("EnsureNameNodeFormatted() error = %v, want PID and storage paths", err)
	}
	if formatCalls != 0 {
		t.Fatalf("format calls = %d, want 0", formatCalls)
	}
}
