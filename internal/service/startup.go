package service

import (
	"context"
	"errors"
	"fmt"
)

// StartResult records only the components started by the current invocation.
// Callers use it to avoid stopping healthy components that predated a failed
// multi-service start.
type StartResult struct {
	Started []string
}

// Started reports whether at least one component was started by this run.
func (r StartResult) StartedAny() bool {
	return len(r.Started) > 0
}

// StartStep is one dependency-ordered component of a transactional start.
// Start returns true only when it created a new process. Ready must validate
// both newly started and pre-existing processes before the step is accepted.
type StartStep struct {
	Name  string
	Start func(context.Context) (bool, error)
	Ready func(context.Context) error
	Stop  func() error
}

// RunStartSteps starts and validates components in dependency order. Any
// failure rolls back newly started components in reverse order.
func RunStartSteps(ctx context.Context, steps []StartStep) (StartResult, error) {
	result := StartResult{}
	started := make([]StartStep, 0, len(steps))

	for _, step := range steps {
		if err := ctx.Err(); err != nil {
			return StartResult{}, rollbackStart(err, started)
		}

		wasStarted, err := step.Start(ctx)
		if wasStarted {
			result.Started = append(result.Started, step.Name)
			started = append(started, step)
		}
		if err != nil {
			return StartResult{}, rollbackStart(fmt.Errorf("start %s: %w", step.Name, err), started)
		}

		if step.Ready != nil {
			if err := step.Ready(ctx); err != nil {
				return StartResult{}, rollbackStart(fmt.Errorf("%s readiness failed: %w", step.Name, err), started)
			}
		}
	}

	return result, nil
}

// RollbackStarted stops the named components in reverse start order. The stop
// callback must be idempotent and ownership-safe.
func RollbackStarted(result StartResult, stop func(string) error) error {
	var cleanupErr error
	for i := len(result.Started) - 1; i >= 0; i-- {
		name := result.Started[i]
		if err := stop(name); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("rollback %s: %w", name, err))
		}
	}
	return cleanupErr
}

func rollbackStart(primary error, started []StartStep) error {
	var cleanupErr error
	for i := len(started) - 1; i >= 0; i-- {
		step := started[i]
		if step.Stop == nil {
			continue
		}
		if err := step.Stop(); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("rollback %s: %w", step.Name, err))
		}
	}
	return errors.Join(primary, cleanupErr)
}
