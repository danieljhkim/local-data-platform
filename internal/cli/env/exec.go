package env

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

func newExecCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exec -- <command> [args...]",
		Short: "Run a command with the local-data-platform environment",
		Long: `Execute a command with the computed environment variables set.

The environment includes HADOOP_CONF_DIR, HIVE_CONF_DIR, SPARK_CONF_DIR, PATH,
and other variables configured for the active profile.

Note: Use '--' to separate env exec flags from the command being executed.

Examples:
  local-data env exec -- hdfs dfs -ls /
  local-data env exec -- hive --version
  local-data env exec -- pyspark`,
		DisableFlagParsing: true, // Don't parse flags after 'exec'
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			// Skip the first arg if it's "--"
			if len(args) > 0 && args[0] == "--" {
				args = args[1:]
			}

			// Execute command
			return envpkg.Exec(paths, args)
		},
	}

	return cmd
}
