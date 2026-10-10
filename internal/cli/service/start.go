package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/danieljhkim/local-data-platform/internal/config"
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	lifecyclesvc "github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/service/yarn"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

type managedService interface {
	StartContext(context.Context) (lifecyclesvc.StartResult, error)
	Rollback(lifecyclesvc.StartResult) error
}

type managedServiceFactory func(*config.Paths) (managedService, error)

type startFactories struct {
	hdfs managedServiceFactory
	yarn managedServiceFactory
	hive managedServiceFactory
}

func defaultStartFactories() startFactories {
	return startFactories{
		hdfs: func(paths *config.Paths) (managedService, error) { return hdfs.NewHDFSService(paths) },
		yarn: func(paths *config.Paths) (managedService, error) { return yarn.NewYARNService(paths) },
		hive: func(paths *config.Paths) (managedService, error) { return hive.NewHiveService(paths) },
	}
}

func newStartCmd(pathsGetter PathsGetter) *cobra.Command {
	return newStartCmdWithFactories(pathsGetter, defaultStartFactories())
}

func newStartCmdWithFactories(pathsGetter PathsGetter, factories startFactories) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start [service]",
		Short: "Start one or all services",
		Long: `Start HDFS, YARN, or Hive services.

With no arguments:
  - hdfs profile: starts all services in order: HDFS → YARN → Hive
  - local profile: starts only Hive (no HDFS/YARN needed)

With a service name, starts only that service.

Examples:
  local-data start           # Start all services for current profile
  local-data start hdfs      # Start HDFS only
  local-data start yarn      # Start YARN only
  local-data start hive      # Start Hive only`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runCtx, stop := cancelOnSignals(ctx(cmd), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return signalCause(runCtx, runStart(runCtx, pathsGetter(), args, factories))
		},
	}

	return cmd
}

func runStart(ctx context.Context, paths *config.Paths, args []string, factories startFactories) error {
	target := ""
	if len(args) > 0 {
		target = args[0]
	}

	// Get active profile to determine which services to start
	profile, _ := paths.ActiveProfile()

	switch target {
	case "":
		// Start services based on profile
		if profile == "local" {
			return startProfile(ctx, paths, []profileStart{{"hive", factories.hive}})
		} else {
			return startProfile(ctx, paths, []profileStart{
				{"hdfs", factories.hdfs},
				{"yarn", factories.yarn},
				{"hive", factories.hive},
			})
		}

	case "hdfs":
		return startOne(ctx, paths, "HDFS", factories.hdfs)

	case "yarn":
		return startOne(ctx, paths, "YARN", factories.yarn)

	case "hive":
		return startOne(ctx, paths, "Hive", factories.hive)

	default:
		return fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
	}
}

type profileStart struct {
	name    string
	factory managedServiceFactory
}

func startProfile(ctx context.Context, paths *config.Paths, specs []profileStart) error {
	steps := make([]lifecyclesvc.StartStep, 0, len(specs))
	for index, spec := range specs {
		index, spec := index, spec
		var svc managedService
		var result lifecyclesvc.StartResult
		steps = append(steps, lifecyclesvc.StartStep{
			Name: spec.name,
			Start: func(ctx context.Context) (bool, error) {
				if index > 0 {
					fmt.Println()
				}
				util.Section("start %s", spec.name)
				var err error
				svc, err = spec.factory(paths)
				if err != nil {
					return false, fmt.Errorf("failed to create %s service: %w", spec.name, err)
				}
				result, err = svc.StartContext(ctx)
				return result.StartedAny(), err
			},
			Stop: func() error {
				if svc == nil {
					return nil
				}
				return svc.Rollback(result)
			},
		})
	}
	_, err := lifecyclesvc.RunStartSteps(ctx, steps)
	return err
}

func startOne(ctx context.Context, paths *config.Paths, label string, factory managedServiceFactory) error {
	svc, err := factory(paths)
	if err != nil {
		return fmt.Errorf("failed to create %s service: %w", label, err)
	}
	_, err = svc.StartContext(ctx)
	return err
}

func ctx(cmd *cobra.Command) context.Context {
	if cmd.Context() != nil {
		return cmd.Context()
	}
	return context.Background()
}

// cancelOnSignals returns a context canceled by the first of sigs, with a
// *envpkg.SignalError as its cause. Cancellation makes RunStartSteps roll back
// what this invocation launched; services run in their own session, so the
// signal reaches only the CLI. The handler is removed after the first signal,
// so a second one terminates the CLI with the default action.
func cancelOnSignals(parent context.Context, sigs ...os.Signal) (context.Context, func()) {
	runCtx, cancel := context.WithCancelCause(parent)
	received := make(chan os.Signal, 1)
	signal.Notify(received, sigs...)
	done := make(chan struct{})
	go func() {
		select {
		case sig := <-received:
			signal.Stop(received)
			sysSig, _ := sig.(syscall.Signal)
			cancel(&envpkg.SignalError{Signal: sysSig})
		case <-done:
		}
	}()
	return runCtx, func() {
		close(done)
		signal.Stop(received)
		cancel(nil)
	}
}

// signalCause attributes err to the signal that canceled ctx, if any, so the
// CLI reports the interrupt and exits with 128 plus the signal number.
func signalCause(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var signalErr *envpkg.SignalError
	if errors.As(context.Cause(ctx), &signalErr) {
		return &envpkg.SignalError{Signal: signalErr.Signal, Err: err}
	}
	return err
}
