package service

import (
	"fmt"
	"strconv"

	"github.com/danieljhkim/local-data-platform/internal/config"
	svc "github.com/danieljhkim/local-data-platform/internal/service"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/service/yarn"
	"github.com/danieljhkim/local-data-platform/internal/util"
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
					util.Section("hive (local profile)")
					if err := statusHive(paths); err != nil {
						return err
					}
				} else {
					// HDFS profile: show all services
					util.Section("hdfs")
					if err := statusHDFS(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("yarn")
					if err := statusYARN(paths); err != nil {
						return err
					}

					fmt.Println()
					util.Section("hive")
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

// statusRows converts ServiceStatus slices into table rows.
func statusRows(statuses []svc.ServiceStatus) []util.StatusTableRow {
	rows := make([]util.StatusTableRow, 0, len(statuses))
	for _, s := range statuses {
		row := util.StatusTableRow{Name: s.Name}
		if s.Running {
			row.Status = "running"
			row.Detail = "pid " + strconv.Itoa(s.PID)
			row.Ok = true
		} else {
			row.Status = "stopped"
		}
		rows = append(rows, row)
	}
	return rows
}

func statusHDFS(paths *config.Paths) error {
	service, err := hdfs.NewHDFSService(paths)
	if err != nil {
		return fmt.Errorf("failed to create HDFS service: %w", err)
	}

	statuses, err := service.Status()
	if err != nil {
		return err
	}

	util.StatusTable(statusRows(statuses))
	return nil
}

func statusYARN(paths *config.Paths) error {
	service, err := yarn.NewYARNService(paths)
	if err != nil {
		return fmt.Errorf("failed to create YARN service: %w", err)
	}

	statuses, err := service.Status()
	if err != nil {
		return err
	}

	util.StatusTable(statusRows(statuses))
	return nil
}

func statusHive(paths *config.Paths) error {
	service, err := hive.NewHiveService(paths)
	if err != nil {
		return fmt.Errorf("failed to create Hive service: %w", err)
	}

	statuses, err := service.Status()
	if err != nil {
		return err
	}

	// Build process rows
	rows := statusRows(statuses)

	// Build listener rows
	listeners := service.ListenerStatuses()
	for _, ls := range listeners {
		row := util.StatusTableRow{
			Name: fmt.Sprintf("%s:%d", ls.Label, ls.Port),
		}
		if ls.Listening {
			row.Status = "listening"
			row.Detail = fmt.Sprintf("pid %s, cmd %s", ls.PID, ls.Cmd)
			row.Ok = true
		} else {
			row.Status = "not listening"
		}
		rows = append(rows, row)
	}

	util.StatusTable(rows)
	return nil
}
