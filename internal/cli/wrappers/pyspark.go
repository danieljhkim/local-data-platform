package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service/hdfs"
	"github.com/spf13/cobra"
)

// NewPySparkCmd creates the pyspark wrapper command
func NewPySparkCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "pyspark [args...]",
		Short:              "Run PySpark with local-data environment",
		Long:               `Run pyspark with the computed local-data-platform environment.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			// Compute environment
			env, err := envpkg.Compute(paths)
			if err != nil {
				return err
			}

			// Ensure /spark-history directory exists in HDFS before running pyspark
			// This is needed for Spark event logging
			profile, _ := paths.ActiveProfile()
			if profile == "hdfs" {
				hdfs.EnsureSparkHistoryDir(env.MergeWithCurrent())
			}

			cmdArgs := append([]string{"pyspark"}, args...)
			return envpkg.Exec(paths, cmdArgs)
		},
	}

	return cmd
}
