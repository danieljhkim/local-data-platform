package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/spf13/cobra"
)

// NewSparkSubmitCmd creates the spark-submit wrapper command
func NewSparkSubmitCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "spark-submit [args...]",
		Short:              "Run spark-submit with local-data environment",
		Long:               `Run spark-submit with the computed local-data-platform environment.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			// Compute environment
			env, err := envpkg.Compute(paths)
			if err != nil {
				return err
			}

			// Ensure /spark-history directory exists in HDFS before running spark-submit
			// This is needed for Spark event logging
			profile, _ := paths.ActiveProfile()
			if profile == "hdfs" {
				hdfs.EnsureSparkHistoryDir(env.MergeWithCurrent())
			}

			cmdArgs := append([]string{"spark-submit"}, args...)
			return envpkg.Exec(paths, cmdArgs)
		},
	}

	return cmd
}
