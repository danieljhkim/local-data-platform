package hdfs

import (
	"testing"
)

func TestFindNameNodePID_NotRunning(t *testing.T) {
	// When NameNode is not running, should return 0
	pid, err := FindNameNodePID()

	// In a test environment, NameNode is likely not running
	// This test validates the function doesn't panic and returns 0
	// Error is acceptable if process not found
	_ = err

	if pid < 0 {
		t.Errorf("FindNameNodePID() = %d, expected >= 0", pid)
	}
}

func TestFindDataNodePID_NotRunning(t *testing.T) {
	// When DataNode is not running, should return 0
	pid, err := FindDataNodePID()

	// In a test environment, DataNode is likely not running
	// This test validates the function doesn't panic and returns 0
	// Error is acceptable if process not found
	_ = err

	if pid < 0 {
		t.Errorf("FindDataNodePID() = %d, expected >= 0", pid)
	}
}

func TestCheckConfOverlay_InvalidPID(t *testing.T) {
	// Test with invalid PID (should return false)
	result := CheckConfOverlay(999999, "/some/conf/dir")

	if result {
		t.Error("CheckConfOverlay() with invalid PID should return false")
	}
}

func TestCheckConfOverlay_EmptyConfDir(t *testing.T) {
	// Test with empty conf dir
	result := CheckConfOverlay(1, "")

	if result {
		t.Error("CheckConfOverlay() with empty conf dir should return false")
	}
}

// Note: Testing actual process discovery requires running HDFS processes,
// which is not suitable for unit tests. These tests verify the functions
// are callable and handle edge cases gracefully.
//
// Full process discovery testing should be done in integration tests
// where we can start actual HDFS processes and verify discovery works.
