package profile

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newInitCmd(pathsGetter PathsGetter) *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Copy repo profiles into $BASE_DIR for local edits",
		Long: `Initialize editable profiles from repository defaults.

Copies profile templates from the repository into $BASE_DIR/conf/profiles
so they can be edited locally without modifying the repository.

Use --force to overwrite existing profiles with repository defaults.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)
			return pm.Init(force)
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing profiles")

	return cmd
}
