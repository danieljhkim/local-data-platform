package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	svc "github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
)

type fakeStatusCollector struct {
	statuses []svc.ServiceStatus
	err      error
}

func (f fakeStatusCollector) Status() ([]svc.ServiceStatus, error) { return f.statuses, f.err }

type fakeHiveStatusCollector struct {
	fakeStatusCollector
	listeners []hive.ListenerStatus
}

func (f fakeHiveStatusCollector) ListenerStatuses() []hive.ListenerStatus { return f.listeners }

func TestStatusJSONIsCleanAndStructuredForSelectionErrors(t *testing.T) {
	cmd := newStatusCmd(func() *config.Paths { return &config.Paths{BaseDir: t.TempDir()} })
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"missing", "--json"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("status missing --json succeeded, want collection error")
	}
	if strings.Contains(output.String(), "\x1b[") || strings.Contains(output.String(), "STATUS") {
		t.Fatalf("JSON stdout contains presentation output: %q", output.String())
	}
	var report statusReport
	if decodeErr := json.Unmarshal(output.Bytes(), &report); decodeErr != nil {
		t.Fatalf("stdout is not valid JSON: %v; output=%q", decodeErr, output.String())
	}
	if report.SchemaVersion != statusSchemaVersion || len(report.Errors) != 1 || report.Errors[0].Probe != "service_selection" {
		t.Fatalf("report = %#v", report)
	}
}

func TestStatusReportPreservesStoppedAndProbeErrorObservations(t *testing.T) {
	report := statusReport{SchemaVersion: statusSchemaVersion, Profile: "local", Services: []statusService{{Name: "hive", Processes: []statusProcess{{Name: "metastore", Running: false}, {Name: "hiveserver2", Running: true, PID: 42}}, Listeners: []statusListener{{Name: "metastore", Port: 9083, Listening: false}}}}, Errors: []statusCollectionError{{Service: "hive", Component: "hiveserver2", Probe: "process", Message: "permission denied"}}}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded statusReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Services[0].Processes[0].Running || decoded.Services[0].Processes[1].PID != 42 || decoded.Errors[0].Message != "permission denied" {
		t.Fatalf("partial observations were not preserved: %#v", decoded)
	}
}

func TestCollectStatusUsesProfileSelectionAndKeepsPartialErrors(t *testing.T) {
	factories := statusFactories{
		hdfs: func(*config.Paths) (statusCollector, error) {
			return fakeStatusCollector{statuses: []svc.ServiceStatus{{Name: "namenode", Running: true, PID: 7}}}, nil
		},
		yarn: func(*config.Paths) (statusCollector, error) {
			return fakeStatusCollector{err: errors.New("jps unavailable")}, nil
		},
		hive: func(*config.Paths) (hiveStatusCollector, error) {
			return fakeHiveStatusCollector{fakeStatusCollector: fakeStatusCollector{statuses: []svc.ServiceStatus{{Name: "metastore"}}}, listeners: []hive.ListenerStatus{{Label: "metastore", Port: 9083}}}, nil
		},
	}
	paths := &config.Paths{BaseDir: t.TempDir()}
	local := collectStatusWithFactories(paths, "local", "", factories)
	if len(local.Services) != 1 || local.Services[0].Name != "hive" || len(local.Services[0].Listeners) != 1 {
		t.Fatalf("local report = %#v", local)
	}
	hdfs := collectStatusWithFactories(paths, "hdfs", "", factories)
	if len(hdfs.Services) != 3 || hdfs.Services[0].Processes[0].PID != 7 || len(hdfs.Errors) != 1 || hdfs.Errors[0].Service != "yarn" {
		t.Fatalf("hdfs report = %#v", hdfs)
	}
	selected := collectStatusWithFactories(paths, "local", "hdfs", factories)
	if len(selected.Services) != 1 || selected.Services[0].Name != "hdfs" {
		t.Fatalf("selected report = %#v", selected)
	}
}
