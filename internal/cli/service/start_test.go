package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	lifecyclesvc "github.com/danieljhkim/local-data-platform/internal/service"
)

type fakeManagedService struct {
	name        string
	result      lifecyclesvc.StartResult
	startErr    error
	rollbackErr error
	events      *[]string
	onStart     func()
}

func (f *fakeManagedService) StartContext(context.Context) (lifecyclesvc.StartResult, error) {
	*f.events = append(*f.events, "start "+f.name)
	if f.onStart != nil {
		f.onStart()
	}
	return f.result, f.startErr
}

func (f *fakeManagedService) Rollback(result lifecyclesvc.StartResult) error {
	*f.events = append(*f.events, "rollback "+f.name+" "+strings.Join(result.Started, ","))
	return f.rollbackErr
}

func TestStartCommand_LocalProfileStartsOnlyHive(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	var events []string
	factories := fakeFactories(&events,
		&fakeManagedService{name: "hdfs", events: &events},
		&fakeManagedService{name: "yarn", events: &events},
		&fakeManagedService{name: "hive", result: lifecyclesvc.StartResult{Started: []string{"metastore", "hiveserver2"}}, events: &events},
	)

	cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("local profile start error = %v", err)
	}
	if want := []string{"start hive"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestStartCommand_LocalProfileReadinessTimeoutIsNonZero(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	var events []string
	factories := fakeFactories(&events,
		&fakeManagedService{name: "hdfs", events: &events},
		&fakeManagedService{name: "yarn", events: &events},
		&fakeManagedService{name: "hive", startErr: errors.New("metastore listener readiness timeout"), events: &events},
	)

	cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "metastore listener readiness timeout") {
		t.Fatalf("local profile start error = %v, want readiness timeout", err)
	}
	if want := []string{"start hive"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestStartCommand_HDFSProfileFullSuccessAndIdempotentRestart(t *testing.T) {
	paths := testPathsWithProfile(t, "hdfs")
	var events []string
	hdfsSvc := &fakeManagedService{name: "hdfs", events: &events}
	yarnSvc := &fakeManagedService{name: "yarn", events: &events}
	hiveSvc := &fakeManagedService{name: "hive", events: &events}
	factories := fakeFactories(&events, hdfsSvc, yarnSvc, hiveSvc)

	for attempt := 0; attempt < 2; attempt++ {
		cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("HDFS profile start attempt %d error = %v", attempt+1, err)
		}
	}
	want := []string{"start hdfs", "start yarn", "start hive", "start hdfs", "start yarn", "start hive"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want idempotent starts %#v", events, want)
	}
}

func TestStartCommand_HDFSProfileDownstreamFailureRollsBackReverseOrder(t *testing.T) {
	paths := testPathsWithProfile(t, "hdfs")
	var events []string
	factories := fakeFactories(&events,
		&fakeManagedService{name: "hdfs", result: lifecyclesvc.StartResult{Started: []string{"namenode", "datanode"}}, events: &events},
		&fakeManagedService{name: "yarn", result: lifecyclesvc.StartResult{Started: []string{"resourcemanager", "nodemanager"}}, events: &events},
		&fakeManagedService{name: "hive", startErr: errors.New("HiveServer2 readiness timeout"), events: &events},
	)

	cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "HiveServer2 readiness timeout") {
		t.Fatalf("HDFS profile start error = %v, want readiness timeout", err)
	}
	want := []string{
		"start hdfs", "start yarn", "start hive",
		"rollback yarn resourcemanager,nodemanager",
		"rollback hdfs namenode,datanode",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestStartCommand_CancellationRollsBackCompletedUpstream(t *testing.T) {
	paths := testPathsWithProfile(t, "hdfs")
	ctx, cancel := context.WithCancel(context.Background())
	var events []string
	factories := fakeFactories(&events,
		&fakeManagedService{
			name:    "hdfs",
			result:  lifecyclesvc.StartResult{Started: []string{"namenode", "datanode"}},
			events:  &events,
			onStart: cancel,
		},
		&fakeManagedService{name: "yarn", events: &events},
		&fakeManagedService{name: "hive", events: &events},
	)

	cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
	cmd.SetContext(ctx)
	err := cmd.Execute()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("HDFS profile start error = %v, want context.Canceled", err)
	}
	want := []string{"start hdfs", "rollback hdfs namenode,datanode"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestStartCommand_AggregatesDownstreamAndRollbackErrors(t *testing.T) {
	paths := testPathsWithProfile(t, "hdfs")
	cleanupErr := errors.New("cleanup permission denied")
	var events []string
	factories := fakeFactories(&events,
		&fakeManagedService{name: "hdfs", result: lifecyclesvc.StartResult{Started: []string{"namenode"}}, rollbackErr: cleanupErr, events: &events},
		&fakeManagedService{name: "yarn", startErr: errors.New("NodeManager startup failed"), events: &events},
		&fakeManagedService{name: "hive", events: &events},
	)

	cmd := newStartCmdWithFactories(func() *config.Paths { return paths }, factories)
	err := cmd.Execute()
	if !errors.Is(err, cleanupErr) || !strings.Contains(err.Error(), "NodeManager startup failed") {
		t.Fatalf("start error = %v, want joined primary and cleanup errors", err)
	}
}

func fakeFactories(events *[]string, hdfsSvc, yarnSvc, hiveSvc *fakeManagedService) startFactories {
	return startFactories{
		hdfs: func(*config.Paths) (managedService, error) { return hdfsSvc, nil },
		yarn: func(*config.Paths) (managedService, error) { return yarnSvc, nil },
		hive: func(*config.Paths) (managedService, error) { return hiveSvc, nil },
	}
}

func testPathsWithProfile(t *testing.T, profile string) *config.Paths {
	t.Helper()
	paths := &config.Paths{BaseDir: t.TempDir()}
	if err := os.MkdirAll(filepath.Dir(paths.ActiveProfileFile()), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ActiveProfileFile(), []byte(profile+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return paths
}
