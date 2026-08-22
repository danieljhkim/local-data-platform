package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/service/hive"
	"github.com/danieljhkim/local-data-platform/internal/util"
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
		force          bool
		user           string
		dbType         string
		dbURL          string
		dbPassword     string
		dbPasswordFile string
	)

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize local-data profiles and metastore",
		Long: `Initialize local-data profiles and metastore.

This command generates profile configs and bootstraps metastore schema.
Defaults to Derby metastore for zero-setup local usage.

Supply the metastore password with --db-password-file, LOCAL_DATA_DB_PASSWORD,
or the interactive prompt (terminal echo is disabled). --db-password is
deprecated because it places the secret in process arguments and shell history.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)
			sm := config.NewSettingsManager(paths)

			if pm.IsInitialized() && !force {
				if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "==> Profiles already initialized: %s\n", paths.UserProfilesDir()); err != nil {
					return err
				}
				if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "==>   (use: local-data init --force to overwrite)"); err != nil {
					return err
				}
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
			resolvedPassword, err := resolveInitPassword(cmd.ErrOrStderr(), dbPasswordFile, dbPassword)
			if err != nil {
				return err
			}
			if resolvedPassword != "" {
				opts.DBPassword = resolvedPassword
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

			opts.DBUrl, err = confirmInitValue(cmd.OutOrStdout(), reader, "db-url", opts.DBUrl, util.RedactJDBCURL(opts.DBUrl))
			if err != nil {
				return err
			}
			opts.DBPassword, err = confirmInitPassword(cmd.OutOrStdout(), cmd.InOrStdin(), reader, opts.DBPassword)
			if err != nil {
				return err
			}

			if err := metastore.ValidateURL(dbTypeNormalized, opts.DBUrl); err != nil {
				if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: %v\n", err); writeErr != nil {
					return writeErr
				}
				return fmt.Errorf("db-type and db-url must match")
			}

			if err := pm.Init(force, opts); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "\nProfiles directory: %s\n", paths.UserProfilesDir()); err != nil {
				return err
			}

			if err := runMetastoreBootstrap(paths, cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr()); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), "Metastore bootstrap completed."); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&force, "force", false, "Overwrite existing profiles")
	cmd.Flags().StringVar(&user, "user", "", "Override username for template substitution")
	cmd.Flags().StringVar(&dbType, "db-type", "", "Metastore DB type (derby, postgres, mysql)")
	cmd.Flags().StringVar(&dbURL, "db-url", "", "Override Hive metastore database connection URL")
	cmd.Flags().StringVar(&dbPassword, "db-password", "", "Deprecated: places the secret in argv. Prefer --db-password-file, LOCAL_DATA_DB_PASSWORD, or the interactive prompt.")
	cmd.Flags().StringVar(&dbPasswordFile, "db-password-file", "", "Read Hive metastore database password from a file (does not place the secret in argv)")
	_ = cmd.Flags().MarkDeprecated("db-password", "places the secret in process arguments and shell history; use --db-password-file, LOCAL_DATA_DB_PASSWORD, or the interactive prompt")

	return cmd
}

func resolveInitPassword(errOut io.Writer, passwordFile, deprecatedFlag string) (string, error) {
	if passwordFile != "" {
		return util.ReadSecretFile(passwordFile)
	}
	if deprecatedFlag != "" {
		if _, err := fmt.Fprintln(errOut, util.DeprecatedPasswordArgWarning); err != nil {
			return "", err
		}
		return deprecatedFlag, nil
	}
	if envVal := os.Getenv(util.DBPasswordEnvVar); envVal != "" {
		return envVal, nil
	}
	return "", nil
}

func confirmInitPassword(out io.Writer, in io.Reader, reader *bufio.Reader, current string) (string, error) {
	if _, err := fmt.Fprintf(out, "confirm db-password to be: %s\n", maskedInitValue(current)); err != nil {
		return "", err
	}
	if _, err := fmt.Fprint(out, "Press Enter to confirm, or type a new value: "); err != nil {
		return "", err
	}
	value, err := util.ReadSecretLine(reader, in, out)
	if err != nil {
		return "", fmt.Errorf("failed to read db-password confirmation: %w", err)
	}
	if value != "" {
		return value, nil
	}
	return current, nil
}

func confirmInitValue(out io.Writer, reader *bufio.Reader, key, current string, displayValue ...string) (string, error) {
	display := current
	if len(displayValue) > 0 {
		display = displayValue[0]
	}
	if _, err := fmt.Fprintf(out, "confirm %s to be: %s\n", key, display); err != nil {
		return "", err
	}
	if _, err := fmt.Fprint(out, "Press Enter to confirm, or type a new value: "); err != nil {
		return "", err
	}

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

func maskedInitValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return util.RedactedValue
}
