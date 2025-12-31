package wrappers

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

// NewHiveCmd creates the hive wrapper command
func NewHiveCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "hive [args...]",
		Short:              "Run Hive commands with local-data environment.",
		Long:               `Run hive commands with the computed local-data environment. It takes time to start the HiveServer2, so you might need to wait a couple of minutes before the first command.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			beelineBase := []string{"beeline", "-u", "jdbc:hive2://localhost:10000"}
			cmdArgs := append(beelineBase, args...)

			// Set TERM=dumb to work around JNA/JLine terminal issues on Apple Silicon
			extraEnv := map[string]string{
				"TERM": "dumb",
			}

			return envpkg.ExecWithEnv(paths, cmdArgs, extraEnv)
		},
	}

	return cmd
}
