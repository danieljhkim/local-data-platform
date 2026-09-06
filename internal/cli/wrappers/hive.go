package wrappers

import (
	"strings"

	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
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
			environment, err := envpkg.Compute(paths)
			if err != nil {
				return err
			}
			cmdArgs := beelineArgs(hive.HiveServer2JDBCURL(environment.HiveConfDir), args)

			// Set TERM=dumb to work around JNA/JLine terminal issues on Apple Silicon
			extraEnv := map[string]string{
				"TERM": "dumb",
			}

			return envpkg.ExecWithEnv(paths, cmdArgs, extraEnv)
		},
	}

	return cmd
}

func beelineArgs(defaultURL string, args []string) []string {
	beelineBase := []string{"beeline"}
	if !hasBeelineURL(args) {
		beelineBase = append(beelineBase, "-u", defaultURL)
	}
	return append(beelineBase, args...)
}

func hasBeelineURL(args []string) bool {
	for _, arg := range args {
		if arg == "-u" || arg == "--url" || strings.HasPrefix(arg, "-u=") || strings.HasPrefix(arg, "--url=") {
			return true
		}
	}
	return false
}
