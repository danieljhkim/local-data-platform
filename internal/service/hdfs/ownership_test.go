package hdfs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/service"
)

func TestHDFSPIDValidator_RejectsUnrelatedPIDFileProcess(t *testing.T) {
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

	pidFile := filepath.Join(pidDir, "namenode.pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("failed to write PID file: %v", err)
	}

	pm := service.NewProcessManager(pidDir, logDir)
	pm.ValidatePID = hdfsPIDValidator()

	err := pm.Stop("namenode")
	if err == nil {
		t.Fatal("Stop() should reject a non-HDFS process from a NameNode PID file")
	}
	if !strings.Contains(err.Error(), "refusing to stop HDFS namenode") {
		t.Fatalf("Stop() error = %q, want HDFS ownership refusal", err)
	}
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
