package service

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/service/yarn"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

func newStopCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [service]",
		Short: "Stop one or all services",
		Long: `Stop HDFS, YARN, or Hive services.

With no arguments:
  - hdfs profile: stops all services in reverse order: Hive → YARN → HDFS
  - local profile: stops only Hive

With a service name, stops only that service.

Examples:
  local-data stop           # Stop all services for current profile
  local-data stop hdfs      # Stop HDFS only
  local-data stop yarn      # Stop YARN only
  local-data stop hive      # Stop Hive only`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			target := ""
			if len(args) > 0 {
				target = args[0]
			}

			// Get active profile to determine which services to stop
			profile, _ := paths.ActiveProfile()

			switch target {
			case "":
				// Stop services based on profile
				if profile == "local" {
					// Local profile: only stop Hive
					util.Section("stop hive (local profile)")
					if err := stopHive(paths); err != nil {
						return err
					}
				} else {
					// HDFS profile: stop all services in reverse order
					util.Section("stop hive")
					if err := stopHive(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("stop yarn")
					if err := stopYARN(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("stop hdfs")
					if err := stopHDFS(paths); err != nil {
						return err
					}
				}

			case "hdfs":
				return stopHDFS(paths)

			case "yarn":
				return stopYARN(paths)

			case "hive":
				return stopHive(paths)

			default:
				return fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
			}

			return nil
		},
	}

	return cmd
}

func stopHDFS(paths *config.Paths) error {
	svc, err := hdfs.NewHDFSService(paths)
	if err != nil {
		return fmt.Errorf("failed to create HDFS service: %w", err)
	}

	return svc.Stop()
}

func stopYARN(paths *config.Paths) error {
	svc, err := yarn.NewYARNService(paths)
	if err != nil {
		return fmt.Errorf("failed to create YARN service: %w", err)
	}

	return svc.Stop()
}

func stopHive(paths *config.Paths) error {
	svc, err := hive.NewHiveService(paths)
	if err != nil {
		return fmt.Errorf("failed to create Hive service: %w", err)
	}

	return svc.Stop()
}
