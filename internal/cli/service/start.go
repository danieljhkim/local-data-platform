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

func newStartCmd(pathsGetter PathsGetter) *cobra.Command {
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
					// Local profile: only start Hive (uses local filesystem)
					util.Section("start hive (local profile - no HDFS/YARN needed)")
					if err := startHive(paths); err != nil {
						return err
					}
				} else {
					// HDFS profile: start all services in order
					util.Section("start hdfs")
					if err := startHDFS(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("start yarn")
					if err := startYARN(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("start hive")
					if err := startHive(paths); err != nil {
						return err
					}
				}

			case "hdfs":
				return startHDFS(paths)

			case "yarn":
				return startYARN(paths)

			case "hive":
				return startHive(paths)

			default:
				return fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
			}

			return nil
		},
	}

	return cmd
}

func startHDFS(paths *config.Paths) error {
	svc, err := hdfs.NewHDFSService(paths)
	if err != nil {
		return fmt.Errorf("failed to create HDFS service: %w", err)
	}

	return svc.Start()
}

func startYARN(paths *config.Paths) error {
	svc, err := yarn.NewYARNService(paths)
	if err != nil {
		return fmt.Errorf("failed to create YARN service: %w", err)
	}

	return svc.Start()
}

func startHive(paths *config.Paths) error {
	svc, err := hive.NewHiveService(paths)
	if err != nil {
		return fmt.Errorf("failed to create Hive service: %w", err)
	}

	return svc.Start()
}
