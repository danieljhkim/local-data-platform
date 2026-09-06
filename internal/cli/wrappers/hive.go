package wrappers

import (
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/spf13/cobra"
)

type environmentComputer func(*config.Paths) (*envpkg.Environment, error)
type environmentExecutor func(*envpkg.Environment, []string, map[string]string) error

// NewHiveCmd creates the hive wrapper command
func NewHiveCmd(pathsGetter PathsGetter) *cobra.Command {
	return newHiveCmd(pathsGetter, envpkg.Compute, envpkg.ExecWithEnvironment)
}

func newHiveCmd(pathsGetter PathsGetter, compute environmentComputer, execute environmentExecutor) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "hive [args...]",
		Short:              "Run Hive commands with local-data environment.",
		Long:               `Run hive commands with the computed local-data environment. It takes time to start the HiveServer2, so you might need to wait a couple of minutes before the first command.`,
		DisableFlagParsing: true, // Critical: pass all args through
		RunE: func(cmd *cobra.Command, args []string) error {
			return runHive(pathsGetter(), args, compute, execute)
		},
	}

	return cmd
}

func runHive(paths *config.Paths, args []string, compute environmentComputer, execute environmentExecutor) error {
	environment, err := compute(paths)
	if err != nil {
		return err
	}
	cmdArgs := beelineArgs(hive.HiveServer2JDBCURL(environment.HiveConfDir), args)

	// Set TERM=dumb to work around JNA/JLine terminal issues on Apple Silicon.
	return execute(environment, cmdArgs, map[string]string{"TERM": "dumb"})
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
