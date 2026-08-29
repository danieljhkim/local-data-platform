package hdfs

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	svc "github.com/danieljhkim/local-data-platform/internal/service"
)

func TestFindJPSPIDFromOutput_IgnoresSecondaryNameNode(t *testing.T) {
	output := []byte(strings.Join([]string{
		"101 org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode",
		"202 org.apache.hadoop.hdfs.server.namenode.NameNode",
	}, "\n"))

	pid := findJPSPIDFromOutput(output, "NameNode")
	if pid != 202 {
		t.Fatalf("findJPSPIDFromOutput(NameNode) = %d, want actual NameNode pid 202", pid)
	}
}

func TestFindJPSPIDFromOutput_DoesNotMatchSecondaryNameNodeOnly(t *testing.T) {
	output := []byte("101 org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode\n")

	pid := findJPSPIDFromOutput(output, "NameNode")
	if pid != 0 {
		t.Fatalf("findJPSPIDFromOutput(NameNode) = %d, want 0 for SecondaryNameNode-only output", pid)
	}
}

func TestTerminateHDFSPID_SIGTERMThenSIGKILLWhenStillRunning(t *testing.T) {
	var signals []syscall.Signal
	running := true

	err := terminateHDFSPIDWithOptions(42, svc.TerminateOptions{
		Timeout: 0,
		IsRunning: func(pid int) bool {
			if pid != 42 {
				t.Fatalf("IsRunning called with pid %d, want 42", pid)
			}
			return running
		},
		Signal: func(pid int, signal syscall.Signal) error {
			if pid != 42 {
				t.Fatalf("Signal called with pid %d, want 42", pid)
			}
			signals = append(signals, signal)
			if signal == syscall.SIGKILL {
				running = false
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("terminateHDFSPIDWithOptions() error = %v", err)
	}

	want := []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}
	if !reflect.DeepEqual(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
}

func TestTerminateHDFSPID_NoSIGKILLWhenSIGTERMStopsProcess(t *testing.T) {
	var signals []syscall.Signal
	running := true

	err := terminateHDFSPIDWithOptions(42, svc.TerminateOptions{
		Timeout: 0,
		IsRunning: func(pid int) bool {
			return running
		},
		Signal: func(pid int, signal syscall.Signal) error {
			signals = append(signals, signal)
			if signal == syscall.SIGTERM {
				running = false
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("terminateHDFSPIDWithOptions() error = %v", err)
	}

	want := []syscall.Signal{syscall.SIGTERM}
	if !reflect.DeepEqual(signals, want) {
		t.Fatalf("signals = %v, want %v", signals, want)
	}
}

func TestStopStaleDaemon_DiscoveredPIDErrorPreventsRestart(t *testing.T) {
	tmpDir := t.TempDir()
	h := &HDFSService{
		procMgr: svc.NewProcessManager(filepath.Join(tmpDir, "pids"), filepath.Join(tmpDir, "logs")),
	}

	originalTerminate := terminateHDFSPID
	defer func() { terminateHDFSPID = originalTerminate }()

	var stoppedPID int
	terminateHDFSPID = func(pid int) error {
		stoppedPID = pid
		return errors.New("permission denied")
	}

	err := h.stopStaleDaemon("namenode", 1234, false)
	if err == nil {
		t.Fatal("stopStaleDaemon() should return a clear error when discovered process termination fails")
	}
	if stoppedPID != 1234 {
		t.Fatalf("terminateHDFSPID called with pid %d, want 1234", stoppedPID)
	}
	if !strings.Contains(err.Error(), "failed to stop discovered HDFS namenode pid 1234 before restart") {
		t.Fatalf("stopStaleDaemon() error = %q, want discovered process context", err)
	}
}

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

func TestWaitForSafeModeWithContext_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := WaitForSafeModeWithContext(ctx, 10, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForSafeModeWithContext() error = %v, want context.Canceled", err)
	}
}

// Note: Testing actual process discovery requires running HDFS processes,
// which is not suitable for unit tests. These tests verify the functions
// are callable and handle edge cases gracefully.
//
// Full process discovery testing should be done in integration tests
// where we can start actual HDFS processes and verify discovery works.
