package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

// NewYARNCmd creates the yarn wrapper command
func NewYARNCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "yarn [args...]",
		Short:              "Run YARN commands with local-data environment",
		Long:               `Run yarn commands with the computed local-data-platform environment.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			cmdArgs := append([]string{"yarn"}, args...)
			return envpkg.Exec(paths, cmdArgs)
		},
	}

	return cmd
}
