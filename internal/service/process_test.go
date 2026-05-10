package service

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNewProcessManager(t *testing.T) {
	pidDir := "/test/pids"
	logDir := "/test/logs"

	pm := NewProcessManager(pidDir, logDir)

	if pm.PidDir != pidDir {
		t.Errorf("PidDir = %q, want %q", pm.PidDir, pidDir)
	}
	if pm.LogDir != logDir {
		t.Errorf("LogDir = %q, want %q", pm.LogDir, logDir)
	}
}

func TestProcessManager_Start_Success(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Use a simple command that will succeed and exit quickly
	cmd := exec.Command("echo", "test")
	name := "test-process"

	pid, err := pm.Start(name, cmd, "test.log")

	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if pid <= 0 {
		t.Errorf("Start() returned invalid PID = %d", pid)
	}

	// Verify PID file was created
	pidFile := filepath.Join(pidDir, name+".pid")
	if _, err := os.Stat(pidFile); os.IsNotExist(err) {
		t.Error("PID file not created")
	}

	// Verify PID file contains correct PID
	content, _ := os.ReadFile(pidFile)
	pidFromFile, _ := strconv.Atoi(string(content))
	if pidFromFile != pid {
		t.Errorf("PID in file = %d, want %d", pidFromFile, pid)
	}

	// Verify log file was created
	logFile := filepath.Join(logDir, "test.log")
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Error("Log file not created")
	}

	// Wait for process to exit
	cmd.Wait()
}

func TestProcessManager_Stop_ByPID(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Start a long-running process
	cmd := exec.Command("sleep", "10")
	name := "sleep-process"

	_, err := pm.Start(name, cmd, "sleep.log")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Give process time to start
	time.Sleep(100 * time.Millisecond)

	// Stop the process
	err = pm.Stop(name)
	if err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	// Verify PID file was removed
	pidFile := filepath.Join(pidDir, name+".pid")
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Error("PID file not removed after stop")
	}

	// Verify process was actually stopped
	time.Sleep(100 * time.Millisecond)
	if pm.IsRunning(name) {
		t.Error("Process still running after stop")
	}
}

func TestProcessManager_Stop_TrimsPIDWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	cmd := exec.Command("sleep", "10")
	name := "sleep-process"

	startedPID, err := pm.Start(name, cmd, "sleep.log")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	pidFile := filepath.Join(pidDir, name+".pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf(" \n%d\n", startedPID)), 0644); err != nil {
		t.Fatalf("failed to rewrite PID file: %v", err)
	}

	if err := pm.Stop(name); err != nil {
		t.Errorf("Stop() error = %v", err)
	}

	_ = cmd.Wait()
}

func TestProcessManager_Stop_RejectsValidatorMismatchWithoutSignaling(t *testing.T) {
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
	defer func() {
		if isProcessRunning(cmd.Process.Pid) {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()

	pm := NewProcessManager(pidDir, logDir)
	pm.ValidatePID = NewProcessMatchValidatorWithInspector(
		"test",
		map[string][]string{"service": {"expected-service-pattern"}},
		func(pid int) (string, error) {
			return "sleep 30", nil
		},
	)

	pidFile := filepath.Join(pidDir, "service.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("failed to write PID file: %v", err)
	}

	err := pm.Stop("service")
	if err == nil {
		t.Fatal("Stop() should reject a PID that fails ownership validation")
	}
	if !strings.Contains(err.Error(), "refusing to stop test service") {
		t.Fatalf("Stop() error = %q, want ownership refusal", err)
	}
	if !isProcessRunning(cmd.Process.Pid) {
		t.Fatal("unrelated process was signaled")
	}
}

func TestProcessManager_Stop_AlreadyStopped(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Try to stop a process that doesn't exist
	err := pm.Stop("nonexistent")

	// Should not return error for non-existent process
	if err != nil {
		t.Errorf("Stop() should not error for non-existent process, got: %v", err)
	}
}

func TestProcessManager_Status_Running(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Start a long-running process
	cmd := exec.Command("sleep", "5")
	name := "status-test"

	startedPID, err := pm.Start(name, cmd, "status.log")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Give process time to start
	time.Sleep(100 * time.Millisecond)

	// Check status
	pid, err := pm.Status(name)
	if err != nil {
		t.Errorf("Status() error = %v", err)
	}

	if pid != startedPID {
		t.Errorf("Status() = %d, want %d", pid, startedPID)
	}

	if pid == 0 {
		t.Error("Status() returned 0 (not running), expected running process")
	}

	// Cleanup
	pm.Stop(name)
	cmd.Wait()
}

func TestProcessManager_Status_TrimsPIDWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	cmd := exec.Command("sleep", "5")
	name := "status-test"

	startedPID, err := pm.Start(name, cmd, "status.log")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		_ = pm.Stop(name)
		_ = cmd.Wait()
	}()

	pidFile := filepath.Join(pidDir, name+".pid")
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", startedPID)), 0644); err != nil {
		t.Fatalf("failed to rewrite PID file: %v", err)
	}

	pid, err := pm.Status(name)
	if err != nil {
		t.Errorf("Status() error = %v", err)
	}
	if pid != startedPID {
		t.Errorf("Status() = %d, want %d", pid, startedPID)
	}
}

func TestProcessManager_Status_NotRunning(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Check status of process that was never started
	pid, err := pm.Status("never-started")
	if err != nil {
		t.Errorf("Status() error = %v", err)
	}

	if pid != 0 {
		t.Errorf("Status() = %d, want 0 (not running)", pid)
	}
}

func TestProcessManager_IsRunning(t *testing.T) {
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	logDir := filepath.Join(tmpDir, "logs")

	pm := NewProcessManager(pidDir, logDir)

	// Start a long-running process
	cmd := exec.Command("sleep", "5")
	name := "running-test"

	_, err := pm.Start(name, cmd, "running.log")
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Give process time to start
	time.Sleep(100 * time.Millisecond)

	// Check if running
	if !pm.IsRunning(name) {
		t.Error("IsRunning() = false, want true")
	}

	// Stop and check again
	pm.Stop(name)
	cmd.Wait()

	time.Sleep(100 * time.Millisecond)
	if pm.IsRunning(name) {
		t.Error("IsRunning() = true after stop, want false")
	}
}
