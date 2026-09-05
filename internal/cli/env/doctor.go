package env

import (
	"encoding/json"
	"fmt"
	"strings"

	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/spf13/cobra"
)

func newDoctorCmd(pathsGetter PathsGetter) *cobra.Command {
	return newDoctorCmdWithRunner(pathsGetter, envpkg.RunDoctor)
}

func newDoctorCmdWithRunner(pathsGetter PathsGetter, runDoctor func(string) *envpkg.DoctorResult) *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:           "doctor [target...]",
		Short:         "Check required and optional dependencies",
		SilenceUsage:  true,
		SilenceErrors: true,
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
			result := runDoctor(target)
			if jsonOutput {
				if err := json.NewEncoder(cmd.OutOrStdout()).Encode(result.JSONReport()); err != nil {
					return err
				}
			} else {
				result.PrintTo(cmd.OutOrStdout())
			}
			if result.ExitCode() != 0 {
				return fmt.Errorf("required dependencies are missing")
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "emit a machine-readable JSON dependency report")

	return cmd
}
