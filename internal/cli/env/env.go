package env

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

// PathsGetter is a function that returns the Paths instance
type PathsGetter func() *config.Paths

// NewEnvCmd creates the env command with all subcommands
func NewEnvCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "env",
		Short: "Environment management commands",
		Long: `Commands for managing and inspecting the local-data-platform environment.

Includes dependency checking, environment variable printing, and hermetic command execution.`,
	}

	// Add subcommands
	cmd.AddCommand(newDoctorCmd(pathsGetter))
	cmd.AddCommand(newPrintCmd(pathsGetter))
	cmd.AddCommand(newExecCmd(pathsGetter))

	return cmd
}
