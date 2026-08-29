package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
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

	cmd := exec.Command("sleep", "10")
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

	if err := pm.Stop(name); err != nil {
		t.Fatalf("Stop() cleanup error = %v", err)
	}
	_ = cmd.Wait()
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
	if err := pm.Stop(name); err != nil {
		t.Fatalf("Stop() cleanup error = %v", err)
	}
	// The process is expected to exit because Stop sends SIGTERM.
	_ = cmd.Wait()
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
	if err := pm.Stop(name); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	// The process is expected to exit because Stop sends SIGTERM.
	_ = cmd.Wait()

	time.Sleep(100 * time.Millisecond)
	if pm.IsRunning(name) {
		t.Error("IsRunning() = true after stop, want false")
	}
}

func TestProcessManager_Start_ImmediateExitIsFailedAndReaped(t *testing.T) {
	tmpDir := t.TempDir()
	pm := NewProcessManager(filepath.Join(tmpDir, "pids"), filepath.Join(tmpDir, "logs"))
	cmd := exec.Command("sh", "-c", "exit 23")

	pid, err := pm.Start("immediate-exit", cmd, "immediate-exit.log")
	if err == nil {
		t.Fatal("Start() should reject a child that exits immediately")
	}
	if pid != 0 {
		t.Fatalf("Start() pid = %d, want 0", pid)
	}
	if !strings.Contains(err.Error(), "exited with status 23") {
		t.Fatalf("Start() error = %q, want exit status", err)
	}
	assertPIDFileMissing(t, pm, "immediate-exit")
	assertChildReaped(t, cmd.Process.Pid)
}

func TestProcessManager_Start_PIDPublishFailureCleansChildAndFile(t *testing.T) {
	tmpDir := t.TempDir()
	pm := NewProcessManager(filepath.Join(tmpDir, "pids"), filepath.Join(tmpDir, "logs"))
	publishErr := errors.New("injected publish failure")
	pm.hooks.writePIDFile = func(path string, data []byte, mode os.FileMode) error {
		if err := os.WriteFile(path, data, mode); err != nil {
			return err
		}
		return publishErr
	}
	cmd := exec.Command("sleep", "30")

	pid, err := pm.Start("publish-failure", cmd, "publish-failure.log")
	if pid != 0 {
		t.Fatalf("Start() pid = %d, want 0", pid)
	}
	if !errors.Is(err, publishErr) {
		t.Fatalf("Start() error = %v, want publish failure", err)
	}
	assertPIDFileMissing(t, pm, "publish-failure")
	assertChildReaped(t, cmd.Process.Pid)
}

func TestProcessManager_Start_JoinsPrimaryAndCleanupFailures(t *testing.T) {
	tmpDir := t.TempDir()
	pm := NewProcessManager(filepath.Join(tmpDir, "pids"), filepath.Join(tmpDir, "logs"))
	closeErr := errors.New("injected close failure")
	cleanupErr := errors.New("injected cleanup failure")
	pm.hooks.closeLogFile = func(file *os.File) error {
		if err := file.Close(); err != nil {
			return err
		}
		return closeErr
	}
	pm.hooks.cleanupStartedProcess = func(cmd *exec.Cmd) error {
		return errors.Join(terminateAndReapStartedProcess(cmd), cleanupErr)
	}
	cmd := exec.Command("sleep", "30")

	_, err := pm.Start("close-failure", cmd, "close-failure.log")
	if !errors.Is(err, closeErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("Start() error = %v, want both close and cleanup failures", err)
	}
	assertPIDFileMissing(t, pm, "close-failure")
	assertChildReaped(t, cmd.Process.Pid)
}

func TestProcessManager_Start_HealthFailureRemovesPublishedPID(t *testing.T) {
	tmpDir := t.TempDir()
	pm := NewProcessManager(filepath.Join(tmpDir, "pids"), filepath.Join(tmpDir, "logs"))
	healthErr := errors.New("injected health check failure")
	removeErr := errors.New("injected remove failure")
	pm.hooks.checkStartedProcess = func(*exec.Cmd) (bool, bool, error) {
		return false, false, healthErr
	}
	pm.hooks.removePIDFile = func(path string) error {
		if err := os.Remove(path); err != nil {
			return err
		}
		return removeErr
	}
	cmd := exec.Command("sleep", "30")

	_, err := pm.Start("health-failure", cmd, "health-failure.log")
	if !errors.Is(err, healthErr) || !errors.Is(err, removeErr) {
		t.Fatalf("Start() error = %v, want health and PID cleanup failures", err)
	}
	assertPIDFileMissing(t, pm, "health-failure")
	assertChildReaped(t, cmd.Process.Pid)
}

func TestWritePIDFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "service.pid")
	if err := writePIDFileAtomic(path, []byte("12345"), 0644); err != nil {
		t.Fatalf("writePIDFileAtomic() error = %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(content) != "12345" {
		t.Fatalf("PID content = %q, want %q", content, "12345")
	}
	temps, err := filepath.Glob(filepath.Join(dir, ".service.pid.tmp-*"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("temporary PID files remain: %v", temps)
	}
}

func TestProcessManager_Stop_TimeoutEscalatesAndValidatesEachSignal(t *testing.T) {
	pm, pidPath := newInjectedStopManager(t, 4242)
	clock := &fakeProcessClock{now: time.Unix(100, 0)}
	pm.Now = clock.Now
	pm.Sleep = clock.Sleep
	pm.StopTimeout = time.Second
	pm.StopPollInterval = time.Second
	running := true
	pm.CheckRunning = func(int) (bool, error) { return running, nil }
	var events []string
	pm.ValidatePID = func(string, int) error {
		events = append(events, "validate")
		return nil
	}
	pm.Signal = func(_ int, signal syscall.Signal) error {
		events = append(events, signalName(signal))
		if signal == syscall.SIGKILL {
			running = false
		}
		return nil
	}

	if err := pm.Stop("service"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	want := []string{"validate", "SIGTERM", "validate", "SIGKILL"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("PID file still exists after confirmed exit: %v", err)
	}
}

func TestProcessManager_Stop_GracefulSuccessRemovesPIDAfterConfirmation(t *testing.T) {
	pm, pidPath := newInjectedStopManager(t, 4242)
	running := true
	pm.CheckRunning = func(int) (bool, error) { return running, nil }
	var signals []syscall.Signal
	pm.Signal = func(_ int, signal syscall.Signal) error {
		signals = append(signals, signal)
		running = false
		return nil
	}

	if err := pm.Stop("service"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals = %v, want SIGTERM only", signals)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("PID file still exists after confirmed exit: %v", err)
	}
}

func TestProcessManager_Stop_PIDReuseRetainsPIDWithoutSignaling(t *testing.T) {
	pm, pidPath := newInjectedStopManager(t, 4242)
	pm.CheckRunning = func(int) (bool, error) { return true, nil }
	reuseErr := errors.New("pid belongs to another process")
	pm.ValidatePID = func(string, int) error { return reuseErr }
	signals := 0
	pm.Signal = func(int, syscall.Signal) error {
		signals++
		return nil
	}

	err := pm.Stop("service")
	if !errors.Is(err, reuseErr) {
		t.Fatalf("Stop() error = %v, want ownership error", err)
	}
	if signals != 0 {
		t.Fatalf("signals = %d, want 0", signals)
	}
	assertPathExists(t, pidPath)
}

func TestProcessManager_Stop_ESRCHRequiresConfirmedExit(t *testing.T) {
	pm, pidPath := newInjectedStopManager(t, 4242)
	running := true
	pm.CheckRunning = func(int) (bool, error) { return running, nil }
	pm.Signal = func(int, syscall.Signal) error {
		running = false
		return syscall.ESRCH
	}

	if err := pm.Stop("service"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Fatalf("PID file still exists after ESRCH and confirmed exit: %v", err)
	}
}

func TestProcessManager_Stop_FailuresRetainPID(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*ProcessManager, error)
	}{
		{
			name: "inspection",
			configure: func(pm *ProcessManager, injected error) {
				pm.CheckRunning = func(int) (bool, error) { return false, injected }
			},
		},
		{
			name: "signal",
			configure: func(pm *ProcessManager, injected error) {
				pm.CheckRunning = func(int) (bool, error) { return true, nil }
				pm.Signal = func(int, syscall.Signal) error { return injected }
			},
		},
		{
			name: "confirmation",
			configure: func(pm *ProcessManager, injected error) {
				checks := 0
				pm.CheckRunning = func(int) (bool, error) {
					checks++
					if checks >= 3 {
						return false, injected
					}
					return true, nil
				}
				pm.Signal = func(int, syscall.Signal) error { return nil }
			},
		},
		{
			name: "still alive after kill",
			configure: func(pm *ProcessManager, _ error) {
				clock := &fakeProcessClock{now: time.Unix(100, 0)}
				pm.Now = clock.Now
				pm.Sleep = clock.Sleep
				pm.StopTimeout = time.Second
				pm.StopPollInterval = time.Second
				pm.CheckRunning = func(int) (bool, error) { return true, nil }
				pm.Signal = func(int, syscall.Signal) error { return nil }
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pm, pidPath := newInjectedStopManager(t, 4242)
			injected := errors.New("injected " + test.name + " failure")
			test.configure(pm, injected)
			err := pm.Stop("service")
			if err == nil {
				t.Fatal("Stop() error = nil, want failure")
			}
			if test.name != "still alive after kill" && !errors.Is(err, injected) {
				t.Fatalf("Stop() error = %v, want injected failure", err)
			}
			assertPathExists(t, pidPath)
		})
	}
}

type fakeProcessClock struct {
	now time.Time
}

func (clock *fakeProcessClock) Now() time.Time {
	return clock.now
}

func (clock *fakeProcessClock) Sleep(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

func newInjectedStopManager(t *testing.T, pid int) (*ProcessManager, string) {
	t.Helper()
	tmpDir := t.TempDir()
	pidDir := filepath.Join(tmpDir, "pids")
	if err := os.MkdirAll(pidDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	pidPath := filepath.Join(pidDir, "service.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return NewProcessManager(pidDir, filepath.Join(tmpDir, "logs")), pidPath
}

func assertPIDFileMissing(t *testing.T, pm *ProcessManager, name string) {
	t.Helper()
	path := filepath.Join(pm.PidDir, name+".pid")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("PID file %s still exists: %v", path, err)
	}
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to remain: %v", path, err)
	}
}

func assertChildReaped(t *testing.T, pid int) {
	t.Helper()
	var status syscall.WaitStatus
	waitedPID, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	if !errors.Is(err, syscall.ECHILD) {
		t.Fatalf("Wait4(%d) = (%d, %v), want ECHILD after reap", pid, waitedPID, err)
	}
}
