package service

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRunStartSteps_RollsBackNewComponentsInReverseOrder(t *testing.T) {
	var events []string
	steps := []StartStep{
		fakeStartStep("existing", false, nil, &events),
		fakeStartStep("upstream", true, nil, &events),
		fakeStartStep("downstream", false, errors.New("fault injected"), &events),
	}

	_, err := RunStartSteps(context.Background(), steps)
	if err == nil || !strings.Contains(err.Error(), "start downstream: fault injected") {
		t.Fatalf("RunStartSteps() error = %v, want downstream context", err)
	}
	want := []string{"start existing", "ready existing", "start upstream", "ready upstream", "start downstream", "stop upstream"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRunStartSteps_AggregatesPrimaryAndCleanupErrors(t *testing.T) {
	cleanupErr := errors.New("cleanup denied")
	steps := []StartStep{
		{
			Name:  "upstream",
			Start: func(context.Context) (bool, error) { return true, nil },
			Stop:  func() error { return cleanupErr },
		},
		{
			Name:  "downstream",
			Start: func(context.Context) (bool, error) { return false, errors.New("startup failed") },
		},
	}

	_, err := RunStartSteps(context.Background(), steps)
	if !errors.Is(err, cleanupErr) || !strings.Contains(err.Error(), "startup failed") || !strings.Contains(err.Error(), "rollback upstream") {
		t.Fatalf("RunStartSteps() error = %v, want joined primary and cleanup errors", err)
	}
}

func TestRunStartSteps_CancellationRollsBackStartedComponent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var stopped bool
	steps := []StartStep{
		{
			Name: "upstream",
			Start: func(context.Context) (bool, error) {
				cancel()
				return true, nil
			},
			Ready: func(ctx context.Context) error { return ctx.Err() },
			Stop:  func() error { stopped = true; return nil },
		},
	}

	_, err := RunStartSteps(ctx, steps)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunStartSteps() error = %v, want context.Canceled", err)
	}
	if !stopped {
		t.Fatal("started component was not rolled back after cancellation")
	}
}

func fakeStartStep(name string, started bool, startErr error, events *[]string) StartStep {
	return StartStep{
		Name: name,
		Start: func(context.Context) (bool, error) {
			*events = append(*events, "start "+name)
			return started, startErr
		},
		Ready: func(context.Context) error {
			*events = append(*events, "ready "+name)
			return nil
		},
		Stop: func() error {
			*events = append(*events, "stop "+name)
			return nil
		},
	}
}
