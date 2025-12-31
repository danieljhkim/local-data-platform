package profile

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newInitCmd(pathsGetter PathsGetter) *cobra.Command {
	var (
		force      bool
		sourceRepo bool
		profileDir string
		user       string
	)

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize profiles for local-data-platform",
		Long: `Initialize profiles for local-data-platform.

By default, profiles are generated using Go struct definitions with the
current user and base directory embedded in the configuration files.

Use --source repo to copy profiles from the repository instead (legacy behavior).

Examples:
  # Generate profiles with defaults (recommended)
  local-data profile init

  # Regenerate profiles, overwriting existing ones
  local-data profile init --force

  # Generate with custom user (embedded in configs)
  local-data profile init --user daniel

  # Use repository profiles directly (legacy behavior)
  local-data profile init --source repo

  # Use custom profile directory with repo source
  local-data profile init --source repo --profile-dir /path/to/profiles`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			opts := config.InitOptions{
				Force:      force,
				SourceRepo: sourceRepo,
				ProfileDir: profileDir,
				User:       user,
			}

			if err := pm.Init(opts); err != nil {
				return err
			}

			fmt.Printf("\nProfiles directory: %s\n", paths.UserProfilesDir())
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing profiles")
	cmd.Flags().BoolVar(&sourceRepo, "source", false, "Use repository profiles (legacy behavior)")
	cmd.Flags().StringVar(&profileDir, "profile-dir", "", "Custom profile directory (only with --source)")
	cmd.Flags().StringVar(&user, "user", "", "Override username for template substitution")

	return cmd
}
