package profile

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

// PathsGetter is a function that returns the Paths instance
type PathsGetter func() *config.Paths

// NewProfileCmd creates the profile command with all subcommands
func NewProfileCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage configuration profiles",
		Long: `Manage configuration profiles for local-data-platform.

Profiles allow you to switch between different configurations (e.g., 'local' vs 'hdfs').`,
	}

	// Add subcommands
	cmd.AddCommand(newInitCmd(pathsGetter))
	cmd.AddCommand(newListCmd(pathsGetter))
	cmd.AddCommand(newSetCmd(pathsGetter))
	cmd.AddCommand(newCheckCmd(pathsGetter))

	return cmd
}
