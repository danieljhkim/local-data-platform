package service

import (
	"context"
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
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
			paths := pathsGetter()

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
					return startProfile(ctx(cmd), paths, []profileStart{{"hive", factories.hive}})
				} else {
					return startProfile(ctx(cmd), paths, []profileStart{
						{"hdfs", factories.hdfs},
						{"yarn", factories.yarn},
						{"hive", factories.hive},
					})
				}

			case "hdfs":
				return startOne(ctx(cmd), paths, "HDFS", factories.hdfs)

			case "yarn":
				return startOne(ctx(cmd), paths, "YARN", factories.yarn)

			case "hive":
				return startOne(ctx(cmd), paths, "Hive", factories.hive)

			default:
				return fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
			}
		},
	}

	return cmd
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
