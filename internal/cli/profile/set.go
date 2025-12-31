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
local edits in $BASE_DIR/conf/profiles.

Examples:
  local-data profile set local
  local-data profile set hdfs
  local-data profile set hdfs --from-repo`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			profileName := args[0]
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			// Check if profiles have been initialized
			if !pm.IsInitialized() && !fromRepo {
				return fmt.Errorf("profiles have not been initialized\n\nRun: local-data profile init")
			}

			// Check if the profile is valid
			profiles, err := pm.List()
			if err != nil && !fromRepo {
				return fmt.Errorf("failed to list profiles: %w", err)
			}

			if !fromRepo {
				found := false
				for _, p := range profiles {
					if p == profileName {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("unknown profile '%s'\n\nAvailable profiles: %v\nRun: local-data profile list", profileName, profiles)
				}
			}

			// Check if the profile is already set
			currentProfile, err := paths.ActiveProfile()
			if err == nil && currentProfile == profileName {
				fmt.Printf("Profile '%s' is already active.\n", profileName)
				fmt.Printf("Runtime config overlay: %s\n", paths.CurrentConfDir())
				return nil
			}

			// Set the profile
			if err := pm.Set(profileName, fromRepo); err != nil {
				return err
			}

			fmt.Printf("\nProfile '%s' is now active.\n", profileName)
			fmt.Printf("Runtime config overlay: %s\n", paths.CurrentConfDir())

			return nil
		},
	}

	cmd.Flags().BoolVar(&fromRepo, "from-repo", false, "Use repo profiles directly (bypass local edits)")

	return cmd
}
