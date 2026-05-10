package hive

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/service"
)

func TestHiveServiceStop_RejectsUnrelatedPIDFileProcess(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	if err := os.MkdirAll(pidDir, 0755); err != nil {
		t.Fatalf("failed to create pid dir: %v", err)
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start harmless process: %v", err)
	}
	defer cleanupProcess(cmd)

	pidFile := filepath.Join(pidDir, "hiveserver2.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("failed to write PID file: %v", err)
	}

	pm := service.NewProcessManager(pidDir, logDir)
	pm.ValidatePID = hivePIDValidator()
	h := &HiveService{procMgr: pm}

	if err := h.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !processRunning(cmd) {
		t.Fatal("unrelated process was signaled")
	}
}

func TestHiveForceStopPidFiles_RejectsUnrelatedPIDFileProcess(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")

	if err := os.MkdirAll(pidDir, 0755); err != nil {
		t.Fatalf("failed to create pid dir: %v", err)
	}

	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start harmless process: %v", err)
	}
	defer cleanupProcess(cmd)

	pidFile := filepath.Join(pidDir, "metastore.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("failed to write PID file: %v", err)
	}

	stopViaPidFiles(pidDir)

	if !processRunning(cmd) {
		t.Fatal("unrelated process was signaled")
	}
}

func processRunning(cmd *exec.Cmd) bool {
	if cmd.Process == nil {
		return false
	}
	return cmd.Process.Signal(syscall.Signal(0)) == nil
}

func cleanupProcess(cmd *exec.Cmd) {
	if processRunning(cmd) {
		_ = cmd.Process.Kill()
	}
	_ = cmd.Wait()
}
