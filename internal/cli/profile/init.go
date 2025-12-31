package profile

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/spf13/cobra"
)

func newInitCmd(pathsGetter PathsGetter) *cobra.Command {
	var (
		force      bool
		user       string
		dbURL      string
		dbPassword string
	)

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize profiles for local-data-platform",
		Long: `Initialize profiles for local-data-platform.

Profiles are generated using Go struct definitions with the current user
and base directory embedded in the configuration files.

Examples:
  # Generate profiles with defaults
  local-data profile init

  # Regenerate profiles, overwriting existing ones
  local-data profile init --force

  # Generate with custom user (embedded in configs)
  local-data profile init --user daniel

  # Generate with custom database connection
  local-data profile init --db-url "jdbc:postgresql://myhost:5432/hive_metastore" --db-password "secret"`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			opts := &generator.InitOptions{
				User:       user,
				DBUrl:      dbURL,
				DBPassword: dbPassword,
			}

			if err := pm.Init(force, opts); err != nil {
				return err
			}

			fmt.Printf("\nProfiles directory: %s\n", paths.UserProfilesDir())
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing profiles")
	cmd.Flags().StringVar(&user, "user", "", "Override username for template substitution")
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Override Hive metastore database connection URL (e.g., jdbc:postgresql://localhost:5432/metastore)")
	cmd.Flags().StringVar(&dbPassword, "db-password", "", "Override Hive metastore database password")

	return cmd
}
