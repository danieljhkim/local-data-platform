package cli

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/service/yarn"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

// NewLogsCmd creates the logs command
func NewLogsCmd(pathsGetter func() *config.Paths) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Show combined logs from all services",
		Long: `Display the most recent log entries from HDFS, YARN, and Hive services.

This command tails the last 120 lines from each service's log files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			// Show HDFS logs
			util.Section("HDFS Logs")
			hdfsSvc, err := hdfs.NewHDFSService(paths)
			if err != nil {
				fmt.Printf("Error creating HDFS service: %v\n", err)
			} else {
				if err := hdfsSvc.Logs(); err != nil {
					fmt.Printf("Error showing HDFS logs: %v\n", err)
				}
			}

			// Show YARN logs
			util.Section("YARN Logs")
			yarnSvc, err := yarn.NewYARNService(paths)
			if err != nil {
				fmt.Printf("Error creating YARN service: %v\n", err)
			} else {
				if err := yarnSvc.Logs(); err != nil {
					fmt.Printf("Error showing YARN logs: %v\n", err)
				}
			}

			// Show Hive logs
			util.Section("Hive Logs")
			hiveSvc, err := hive.NewHiveService(paths)
			if err != nil {
				fmt.Printf("Error creating Hive service: %v\n", err)
			} else {
				if err := hiveSvc.Logs(); err != nil {
					fmt.Printf("Error showing Hive logs: %v\n", err)
				}
			}

			return nil
		},
	}

	return cmd
}
