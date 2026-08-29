package hdfs

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func TestStartComponents_DataNodeFailureRollsBackNewNameNode(t *testing.T) {
	var stopped []string
	h := newTransactionalTestService(&stopped)
	h.startNameNodeHook = func(context.Context) (bool, error) { return true, nil }
	h.startDataNodeHook = func(context.Context) (bool, error) { return false, errors.New("injected DataNode failure") }

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
	h.startNameNodeHook = func(context.Context) (bool, error) { return false, nil }
	h.startDataNodeHook = func(context.Context) (bool, error) { return false, errors.New("injected DataNode failure") }

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
	h.startNameNodeHook = func(context.Context) (bool, error) { return true, nil }
	h.startDataNodeHook = func(context.Context) (bool, error) { return true, nil }
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
	h.startNameNodeHook = func(context.Context) (bool, error) { return false, nil }
	h.startDataNodeHook = func(context.Context) (bool, error) { return true, nil }

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
