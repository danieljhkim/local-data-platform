package env

import (
	"os"
	"strings"

	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

func newDoctorCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor [target...]",
		Short: "Check required and optional dependencies",
		Long: `Check that all required commands are available.

Optional target can be specified to check context-specific dependencies:
  - "start hdfs"  : Check HDFS dependencies
  - "start yarn"  : Check YARN dependencies
  - "start hive"  : Check Hive dependencies

Examples:
  local-data env doctor
  local-data env doctor start hdfs
  local-data env doctor start hive`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Join args to form target (e.g., ["start", "hdfs"] -> "start hdfs")
			target := strings.Join(args, " ")

			// Run doctor checks
			result := envpkg.RunDoctor(target)

			// Print results
			result.Print()

			// Exit with appropriate code
			os.Exit(result.ExitCode())
			return nil
		},
	}

	return cmd
}
