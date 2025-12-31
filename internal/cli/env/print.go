package env

import (
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

func newPrintCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print",
		Short: "Print export statements for a hermetic environment",
		Long: `Print environment variable export statements.

Output can be evaluated in your shell to set up the local-data-platform environment:

  eval "$(local-data env print)"

This sets HADOOP_CONF_DIR, HIVE_CONF_DIR, SPARK_CONF_DIR, PATH, and other
variables to use the active profile configuration.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			// Compute environment
			env, err := envpkg.Compute(paths)
			if err != nil {
				return err
			}

			// Print shell exports
			env.PrintShell()

			return nil
		},
	}

	return cmd
}
