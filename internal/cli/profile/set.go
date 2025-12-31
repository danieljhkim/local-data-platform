package profile

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newSetCmd(pathsGetter PathsGetter) *cobra.Command {
	var fromRepo bool

	cmd := &cobra.Command{
		Use:   "set <profile-name>",
		Short: "Set active profile and apply runtime config overlay",
		Long: `Set the active profile and materialize its configuration.

The profile configuration is copied to $BASE_DIR/conf/current/ where
it will be used by all services.

Use --from-repo to use repository profiles directly, bypassing any
local edits in $BASE_DIR/conf/profiles.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			profile := args[0]
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			if err := pm.Set(profile, fromRepo); err != nil {
				return err
			}

			fmt.Printf("\nProfile '%s' is now active.\n", profile)
			fmt.Printf("Runtime config overlay: %s\n", paths.CurrentConfDir())

			return nil
		},
	}

	cmd.Flags().BoolVar(&fromRepo, "from-repo", false, "Use repo profiles directly (bypass local edits)")

	return cmd
}
