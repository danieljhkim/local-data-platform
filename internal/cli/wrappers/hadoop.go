package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

// NewHadoopCmd creates the hadoop wrapper command
func NewHadoopCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "hadoop [args...]",
		Short:              "Run Hadoop commands with local-data environment",
		Long:               `Run hadoop commands with the computed local-data-platform environment.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			cmdArgs := append([]string{"hadoop"}, args...)
			return envpkg.Exec(paths, cmdArgs)
		},
	}

	return cmd
}
