package profile

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

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
			sm := config.NewSettingsManager(paths)

			if pm.IsInitialized() && !force {
				fmt.Fprintf(cmd.ErrOrStderr(), "==> Profiles already initialized: %s\n", paths.UserProfilesDir())
				fmt.Fprintln(cmd.ErrOrStderr(), "==>   (use: local-data profile init --force to overwrite)")
				return nil
			}

			settings, err := sm.LoadOrDefault()
			if err != nil {
				return fmt.Errorf("failed to load settings: %w", err)
			}

			opts := &generator.InitOptions{
				User:       settings.User,
				DBUrl:      settings.DBURL,
				DBPassword: settings.DBPassword,
			}

			if user != "" {
				opts.User = user
			}
			if dbURL != "" {
				opts.DBUrl = dbURL
			}
			if dbPassword != "" {
				opts.DBPassword = dbPassword
			}

			reader := bufio.NewReader(cmd.InOrStdin())

			opts.User, err = confirmInitValue(cmd.OutOrStdout(), reader, "user", opts.User)
			if err != nil {
				return err
			}
			opts.DBUrl, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-url", opts.DBUrl)
			if err != nil {
				return err
			}
			opts.DBPassword, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-password", opts.DBPassword)
			if err != nil {
				return err
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

func confirmInitValue(out io.Writer, reader *bufio.Reader, key, current string) (string, error) {
	fmt.Fprintf(out, "confirm %s to be: %s\n", key, current)
	fmt.Fprint(out, "Press Enter to confirm, or type a new value: ")

	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("failed to read %s confirmation: %w", key, err)
	}

	value := strings.TrimSpace(line)
	if value != "" {
		return value, nil
	}

	return current, nil
}
