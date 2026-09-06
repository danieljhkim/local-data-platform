package hdfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/service"
)

func TestStartComponents_DataNodeFailureRollsBackNewNameNode(t *testing.T) {
	var stopped []string
	h := newTransactionalTestService(&stopped)
	h.startNameNodeHook = func(context.Context, []string) (bool, error) { return true, nil }
	h.startDataNodeHook = func(context.Context, []string) (bool, error) { return false, errors.New("injected DataNode failure") }

	_, err := h.startComponents(context.Background(), nil, &config.ServicePaths{LogsDir: "/logs"})
	if err == nil || !strings.Contains(err.Error(), "injected DataNode failure") {
		t.Fatalf("startComponents() error = %v, want DataNode failure", err)
	}
	if want := []string{"namenode"}; !reflect.DeepEqual(stopped, want) {
		t.Fatalf("stopped = %#v, want %#v", stopped, want)
	}
}

func TestStartComponents_DataNodeFailurePreservesExistingNameNode(t *testing.T) {
	var stopped []string
	h := newTransactionalTestService(&stopped)
	h.startNameNodeHook = func(context.Context, []string) (bool, error) { return false, nil }
	h.startDataNodeHook = func(context.Context, []string) (bool, error) { return false, errors.New("injected DataNode failure") }

	_, err := h.startComponents(context.Background(), nil, &config.ServicePaths{LogsDir: "/logs"})
	if err == nil {
		t.Fatal("startComponents() unexpectedly succeeded")
	}
	if len(stopped) != 0 {
		t.Fatalf("pre-existing NameNode was stopped: %#v", stopped)
	}
}

func TestStartComponents_SafeModeTimeoutFailsAndRollsBackReverseOrder(t *testing.T) {
	var stopped []string
	h := newTransactionalTestService(&stopped)
	h.startNameNodeHook = func(context.Context, []string) (bool, error) { return true, nil }
	h.startDataNodeHook = func(context.Context, []string) (bool, error) { return true, nil }
	h.waitSafeModeHook = func(context.Context, int, []string) error {
		return errors.New("HDFS did not exit safe mode after 10 retries")
	}

	_, err := h.startComponents(context.Background(), nil, &config.ServicePaths{LogsDir: "/diagnostic/logs"})
	if err == nil || !strings.Contains(err.Error(), "HDFS did not exit safe mode") || !strings.Contains(err.Error(), "/diagnostic/logs") {
		t.Fatalf("startComponents() error = %v, want actionable safe-mode timeout", err)
	}
	if want := []string{"datanode", "namenode"}; !reflect.DeepEqual(stopped, want) {
		t.Fatalf("stopped = %#v, want reverse order %#v", stopped, want)
	}
}

func TestStartComponents_SuccessRecordsNewDaemons(t *testing.T) {
	h := newTransactionalTestService(nil)
	h.startNameNodeHook = func(context.Context, []string) (bool, error) { return false, nil }
	h.startDataNodeHook = func(context.Context, []string) (bool, error) { return true, nil }

	result, err := h.startComponents(context.Background(), nil, &config.ServicePaths{LogsDir: "/logs"})
	if err != nil {
		t.Fatalf("startComponents() error = %v", err)
	}
	if want := []string{"datanode"}; !reflect.DeepEqual(result.Started, want) {
		t.Fatalf("result.Started = %#v, want %#v", result.Started, want)
	}
}

func newTransactionalTestService(stopped *[]string) *HDFSService {
	h := &HDFSService{
		verifyDaemonHook: func(string) error { return nil },
		waitSafeModeHook: func(context.Context, int, []string) error { return nil },
		createDirsHook:   func([]string) error { return nil },
	}
	if stopped != nil {
		h.stopHook = func(name string) error {
			*stopped = append(*stopped, name)
			return nil
		}
	}
	return h
}

func TestStartComponents_PassesOneRuntimeEnvironmentToEveryStartupStep(t *testing.T) {
	runtimeEnv := []string{"HADOOP_CONF_DIR=/active/overlay", "PATH=/fake/bin"}
	var received [][]string
	h := newTransactionalTestService(nil)
	h.startNameNodeHook = func(_ context.Context, env []string) (bool, error) {
		received = append(received, env)
		return true, nil
	}
	h.startDataNodeHook = func(_ context.Context, env []string) (bool, error) {
		received = append(received, env)
		return true, nil
	}
	h.waitSafeModeHook = func(_ context.Context, _ int, env []string) error {
		received = append(received, env)
		return nil
	}
	h.createDirsHook = func(env []string) error {
		received = append(received, env)
		return nil
	}

	if _, err := h.startComponents(context.Background(), runtimeEnv, &config.ServicePaths{LogsDir: "/logs"}); err != nil {
		t.Fatalf("startComponents() error = %v", err)
	}
	if len(received) != 4 {
		t.Fatalf("startup environment recipients = %d, want 4", len(received))
	}
	for _, env := range received {
		if !reflect.DeepEqual(env, runtimeEnv) {
			t.Fatalf("startup environment = %#v, want %#v", env, runtimeEnv)
		}
	}
}

func TestHDFSStop_RetainsPIDRecordsWhenTerminationIsUncertain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inspectErr error
	}{
		{name: "inspection failure", inspectErr: errors.New("injected inspection failure")},
		{name: "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pidDir := t.TempDir()
			for name, pid := range map[string]string{"datanode": "41", "namenode": "42"} {
				if err := os.WriteFile(filepath.Join(pidDir, name+".pid"), []byte(pid), 0644); err != nil {
					t.Fatal(err)
				}
			}

			inspected := map[int]int{}
			signals := []syscall.Signal{}
			pm := &service.ProcessManager{
				PidDir:      pidDir,
				StopTimeout: time.Nanosecond,
				CheckRunning: func(pid int) (bool, error) {
					inspected[pid]++
					return tc.inspectErr == nil, tc.inspectErr
				},
				Signal: func(_ int, signal syscall.Signal) error {
					signals = append(signals, signal)
					return nil
				},
			}

			err := (&HDFSService{procMgr: pm}).Stop()
			if err == nil || !strings.Contains(err.Error(), "datanode") || !strings.Contains(err.Error(), "namenode") {
				t.Fatalf("Stop() error = %v, want both failed components", err)
			}
			for name := range map[string]struct{}{"datanode": {}, "namenode": {}} {
				if _, statErr := os.Stat(filepath.Join(pidDir, name+".pid")); statErr != nil {
					t.Fatalf("%s PID record was removed after uncertain termination: %v", name, statErr)
				}
			}
			if inspected[41] == 0 || inspected[42] == 0 {
				t.Fatalf("inspected = %#v, want shutdown attempts for both components", inspected)
			}
			if tc.inspectErr == nil && !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL, syscall.SIGTERM, syscall.SIGKILL}) {
				t.Fatalf("signals = %v, want timeout escalation for both components", signals)
			}
		})
	}
}
