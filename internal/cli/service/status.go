package service

import (
	"encoding/json"
	"errors"
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
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:          "status [service]",
		Short:        "Show status of one or all services",
		SilenceUsage: true,
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
			profile, profileErr := paths.ActiveProfile()
			report := collectStatus(paths, profile, target)
			if profileErr != nil {
				report.Errors = append(report.Errors, statusCollectionError{Probe: "active_profile", Message: profileErr.Error()})
			}
			if jsonOutput {
				if err := json.NewEncoder(cmd.OutOrStdout()).Encode(report); err != nil {
					return err
				}
			} else {
				renderStatusReport(report)
			}
			if len(report.Errors) > 0 {
				return fmt.Errorf("status collection failed: %w", errors.New(report.Errors[0].Message))
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit a machine-readable JSON status report")
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

const statusSchemaVersion = 1

type statusProcess struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
	PID     int    `json:"pid,omitempty"`
}
type statusListener struct {
	Name      string `json:"name"`
	Port      int    `json:"port"`
	Listening bool   `json:"listening"`
	PID       string `json:"pid,omitempty"`
}
type statusCollectionError struct {
	Service   string `json:"service"`
	Component string `json:"component,omitempty"`
	Probe     string `json:"probe"`
	Message   string `json:"message"`
}
type statusService struct {
	Name      string           `json:"name"`
	Processes []statusProcess  `json:"processes"`
	Listeners []statusListener `json:"listeners,omitempty"`
}
type statusReport struct {
	SchemaVersion int                     `json:"schema_version"`
	Profile       string                  `json:"profile"`
	Services      []statusService         `json:"services"`
	Errors        []statusCollectionError `json:"errors"`
}

type statusCollector interface {
	Status() ([]svc.ServiceStatus, error)
}

type hiveStatusCollector interface {
	statusCollector
	ListenerStatuses() []hive.ListenerStatus
}

type statusFactories struct {
	hdfs func(*config.Paths) (statusCollector, error)
	yarn func(*config.Paths) (statusCollector, error)
	hive func(*config.Paths) (hiveStatusCollector, error)
}

func defaultStatusFactories() statusFactories {
	return statusFactories{
		hdfs: func(paths *config.Paths) (statusCollector, error) { return hdfs.NewHDFSService(paths) },
		yarn: func(paths *config.Paths) (statusCollector, error) { return yarn.NewYARNService(paths) },
		hive: func(paths *config.Paths) (hiveStatusCollector, error) { return hive.NewHiveService(paths) },
	}
}

func collectStatus(paths *config.Paths, profile, target string) statusReport {
	return collectStatusWithFactories(paths, profile, target, defaultStatusFactories())
}

func collectStatusWithFactories(paths *config.Paths, profile, target string, factories statusFactories) statusReport {
	report := statusReport{SchemaVersion: statusSchemaVersion, Profile: profile, Services: []statusService{}, Errors: []statusCollectionError{}}
	var names []string
	switch target {
	case "":
		if profile == "local" {
			names = []string{"hive"}
		} else {
			names = []string{"hdfs", "yarn", "hive"}
		}
	case "hdfs", "yarn", "hive":
		names = []string{target}
	default:
		report.Errors = append(report.Errors, statusCollectionError{Probe: "service_selection", Message: fmt.Sprintf("unknown service: %s (valid: hdfs, yarn, hive)", target)})
		return report
	}
	for _, name := range names {
		entry := statusService{Name: name, Processes: []statusProcess{}}
		var statuses []svc.ServiceStatus
		var listeners []hive.ListenerStatus
		var err error
		switch name {
		case "hdfs":
			var service statusCollector
			service, err = factories.hdfs(paths)
			if err == nil {
				statuses, err = service.Status()
			}
		case "yarn":
			var service statusCollector
			service, err = factories.yarn(paths)
			if err == nil {
				statuses, err = service.Status()
			}
		case "hive":
			var service hiveStatusCollector
			service, err = factories.hive(paths)
			if err == nil {
				statuses, err = service.Status()
				listeners = service.ListenerStatuses()
			}
		}
		if err != nil {
			report.Errors = append(report.Errors, statusCollectionError{Service: name, Probe: "process_status", Message: err.Error()})
			report.Services = append(report.Services, entry)
			continue
		}
		for _, status := range statuses {
			entry.Processes = append(entry.Processes, statusProcess{Name: status.Name, Running: status.Running, PID: status.PID})
			if status.ProbeError != nil {
				report.Errors = append(report.Errors, statusCollectionError{Service: name, Component: status.Name, Probe: "process", Message: status.ProbeError.Error()})
			}
		}
		for _, listener := range listeners {
			entry.Listeners = append(entry.Listeners, statusListener{Name: listener.Label, Port: listener.Port, Listening: listener.Listening, PID: listener.PID})
			if listener.ProbeError != nil {
				report.Errors = append(report.Errors, statusCollectionError{Service: name, Component: listener.Label, Probe: "listener", Message: listener.ProbeError.Error()})
			}
		}
		report.Services = append(report.Services, entry)
	}
	return report
}

func renderStatusReport(report statusReport) {
	for index, service := range report.Services {
		if index > 0 {
			fmt.Println()
		}
		header := service.Name
		if report.Profile == "local" && service.Name == "hive" {
			header += " (local profile)"
		}
		util.Section("%s", header)
		rows := make([]util.StatusTableRow, 0, len(service.Processes)+len(service.Listeners))
		for _, process := range service.Processes {
			rows = append(rows, reportRow(process.Name, process.Running, process.PID, "running", "stopped"))
		}
		for _, listener := range service.Listeners {
			rows = append(rows, reportRow(fmt.Sprintf("%s:%d", listener.Name, listener.Port), listener.Listening, mustPID(listener.PID), "listening", "not listening"))
		}
		util.StatusTable(rows)
	}
}

func reportRow(name string, active bool, pid int, activeLabel, inactiveLabel string) util.StatusTableRow {
	row := util.StatusTableRow{Name: name, Status: inactiveLabel}
	if active {
		row.Status = activeLabel
		row.Detail = "pid " + strconv.Itoa(pid)
		row.Ok = true
	}
	return row
}

func mustPID(value string) int { pid, _ := strconv.Atoi(value); return pid }
