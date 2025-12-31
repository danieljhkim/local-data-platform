package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

// NewHDFSCmd creates the hdfs wrapper command
func NewHDFSCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "hdfs [args...]",
		Short:              "Run HDFS commands with local-data environment",
		Long:               `Run hdfs commands with the computed local-data-platform environment.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			cmdArgs := append([]string{"hdfs"}, args...)
			return envpkg.Exec(paths, cmdArgs)
		},
	}

	return cmd
}
