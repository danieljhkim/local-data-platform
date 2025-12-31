package service

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/service/yarn"
	"github.com/spf13/cobra"
)

func newStatusCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [service]",
		Short: "Show status of one or all services",
		Long: `Show the status of HDFS, YARN, or Hive services.

With no arguments:
  - hdfs profile: shows status of all services
  - local profile: shows only Hive status

With a service name, shows status of only that service.

Examples:
  local-data status           # Show services for current profile
  local-data status hdfs      # Show HDFS only
  local-data status yarn      # Show YARN only
  local-data status hive      # Show Hive only`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			target := ""
			if len(args) > 0 {
				target = args[0]
			}

			// Get active profile to determine which services to show
			profile, _ := paths.ActiveProfile()

			switch target {
			case "":
				// Show services based on profile
				if profile == "local" {
					// Local profile: only show Hive
					fmt.Println("==> hive (local profile)")
					if err := statusHive(paths); err != nil {
						return err
					}
				} else {
					// HDFS profile: show all services
					fmt.Println("==> hdfs")
					if err := statusHDFS(paths); err != nil {
						return err
					}

					fmt.Println()
					fmt.Println("==> yarn")
					if err := statusYARN(paths); err != nil {
						return err
					}

					fmt.Println()
					fmt.Println("==> hive")
					if err := statusHive(paths); err != nil {
						return err
					}
				}

			case "hdfs":
				return statusHDFS(paths)

			case "yarn":
				return statusYARN(paths)

			case "hive":
				return statusHive(paths)

			default:
				return fmt.Errorf("unknown service: %s (valid: hdfs, yarn, hive)", target)
			}

			return nil
		},
	}

	return cmd
}

func statusHDFS(paths *config.Paths) error {
	svc, err := hdfs.NewHDFSService(paths)
	if err != nil {
		return fmt.Errorf("failed to create HDFS service: %w", err)
	}

	statuses, err := svc.Status()
	if err != nil {
		return err
	}

	for _, status := range statuses {
		if status.Running {
			fmt.Printf("%s: running (pid %d)\n", status.Name, status.PID)
		} else {
			fmt.Printf("%s: stopped\n", status.Name)
		}
	}

	return nil
}

func statusYARN(paths *config.Paths) error {
	svc, err := yarn.NewYARNService(paths)
	if err != nil {
		return fmt.Errorf("failed to create YARN service: %w", err)
	}

	statuses, err := svc.Status()
	if err != nil {
		return err
	}

	for _, status := range statuses {
		if status.Running {
			fmt.Printf("%s: running (pid %d)\n", status.Name, status.PID)
		} else {
			fmt.Printf("%s: stopped\n", status.Name)
		}
	}

	return nil
}

func statusHive(paths *config.Paths) error {
	svc, err := hive.NewHiveService(paths)
	if err != nil {
		return fmt.Errorf("failed to create Hive service: %w", err)
	}

	statuses, err := svc.Status()
	if err != nil {
		return err
	}

	for _, status := range statuses {
		if status.Running {
			fmt.Printf("%s: running (pid %d)\n", status.Name, status.PID)
		} else {
			fmt.Printf("%s: stopped\n", status.Name)
		}
	}

	return nil
}
