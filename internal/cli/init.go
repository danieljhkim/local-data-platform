package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/spf13/cobra"
)

var runMetastoreBootstrap = func(paths *config.Paths, in io.Reader, out, errOut io.Writer) error {
	svc, err := hive.NewHiveService(paths)
	if err != nil {
		return fmt.Errorf("failed to create Hive service: %w", err)
	}
	return svc.BootstrapMetastore(in, out, errOut)
}

func newInitCmd(pathsGetter func() *config.Paths) *cobra.Command {
	var (
		force      bool
		user       string
		dbType     string
		dbURL      string
		dbPassword string
	)

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize local-data profiles and metastore",
		Long: `Initialize local-data profiles and metastore.

This command generates profile configs and bootstraps metastore schema.
Defaults to Derby metastore for zero-setup local usage.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)
			sm := config.NewSettingsManager(paths)

			if pm.IsInitialized() && !force {
				fmt.Fprintf(cmd.ErrOrStderr(), "==> Profiles already initialized: %s\n", paths.UserProfilesDir())
				fmt.Fprintln(cmd.ErrOrStderr(), "==>   (use: local-data init --force to overwrite)")
				return nil
			}

			settings, err := sm.LoadOrDefault()
			if err != nil {
				return fmt.Errorf("failed to load settings: %w", err)
			}

			opts := &generator.InitOptions{
				User:       settings.User,
				DBType:     settings.DBType,
				DBUrl:      settings.DBURL,
				DBPassword: settings.DBPassword,
			}
			if user != "" {
				opts.User = user
			}
			if dbType != "" {
				opts.DBType = dbType
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
			opts.DBType, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-type", opts.DBType)
			if err != nil {
				return err
			}
			dbTypeNormalized, err := metastore.NormalizeDBType(opts.DBType)
			if err != nil {
				return err
			}
			opts.DBType = string(dbTypeNormalized)

			opts.DBUrl, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-url", opts.DBUrl)
			if err != nil {
				return err
			}
			opts.DBPassword, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-password", opts.DBPassword)
			if err != nil {
				return err
			}

			if err := metastore.ValidateURL(dbTypeNormalized, opts.DBUrl); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: %v\n", err)
				return fmt.Errorf("db-type and db-url must match")
			}

			if err := pm.Init(force, opts); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "\nProfiles directory: %s\n", paths.UserProfilesDir())

			if err := runMetastoreBootstrap(paths, cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr()); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Metastore bootstrap completed.")
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing profiles")
	cmd.Flags().StringVar(&user, "user", "", "Override username for template substitution")
	cmd.Flags().StringVar(&dbType, "db-type", "", "Metastore DB type (derby, postgres, mysql)")
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Override Hive metastore database connection URL")
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
