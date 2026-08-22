package setting

import (
	"fmt"
	"os"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

func newSetCmd(pathsGetter PathsGetter) *cobra.Command {
	var (
		fromFile  string
		fromStdin bool
	)

	cmd := &cobra.Command{
		Use:   "set <key> [value]",
		Short: "Set a configurable user setting",
		Long: `Set a configurable user setting.

Supported keys: user, db-type, db-url, db-password.
Note: base-dir is static and cannot be changed via this command.

db-password must not be passed as a command-line argument. Use one of:
  local-data setting set db-password                  # prompt (echo disabled)
  local-data setting set db-password --stdin          # read from stdin
  local-data setting set db-password --from-file PATH
  LOCAL_DATA_DB_PASSWORD=... local-data setting set db-password`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires a setting key")
			}
			if args[0] == "db-password" {
				if len(args) > 2 {
					return fmt.Errorf("too many arguments for db-password")
				}
				return nil
			}
			if len(args) != 2 {
				return fmt.Errorf("requires a key and a value")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			value := ""
			if len(args) > 1 {
				value = args[1]
			}
			if key != "db-password" {
				if fromFile != "" || fromStdin {
					return fmt.Errorf("--from-file and --stdin are only valid for db-password")
				}
			} else {
				resolved, err := resolveDBPassword(cmd, value, fromFile, fromStdin)
				if err != nil {
					return err
				}
				value = resolved
			}
			paths := pathsGetter()

			sm := config.NewSettingsManager(paths)
			settings, err := sm.LoadOrDefault()
			if err != nil {
				return err
			}
			oldValue := settingValue(settings, key)

			switch key {
			case "user":
				settings.User = value
			case "base-dir":
				return fmt.Errorf("base-dir is static and cannot be changed via 'local-data setting set'")
			case "db-type":
				dbType, err := metastore.NormalizeDBType(value)
				if err != nil {
					return err
				}
				settings.DBType = string(dbType)
				if metastore.InferDBTypeFromURL(settings.DBURL) != dbType {
					if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: db-url %q does not match db-type %q; resetting db-url to default.\n", util.RedactJDBCURL(settings.DBURL), settings.DBType); err != nil {
						return err
					}
					settings.DBURL = metastore.DefaultDBURLForBase(dbType, paths.BaseDir)
				}
			case "db-url":
				settings.DBURL = value
			case "db-password":
				settings.DBPassword = value
			default:
				return fmt.Errorf("unknown setting key %q (supported: user, db-type, db-url, db-password)", key)
			}

			dbType, err := metastore.NormalizeDBType(settings.DBType)
			if err != nil {
				return err
			}
			if err := metastore.ValidateURL(dbType, settings.DBURL); err != nil {
				if _, writeErr := fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: %v\n", err); writeErr != nil {
					return writeErr
				}
				return fmt.Errorf("db-type and db-url must match")
			}

			if err := sm.Save(settings); err != nil {
				return err
			}

			applier := config.NewSettingsApplier(paths)
			if err := applier.Apply(key, oldValue, value); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Updated %s in %s\n", key, sm.Path()); err != nil {
				return err
			}
			if _, err := fmt.Fprintln(cmd.ErrOrStderr(), "WARNING: Run 'local-data init --force' to ensure regenerated profiles fully reflect updated settings."); err != nil {
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&fromFile, "from-file", "", "Read db-password from a file (does not place the secret in argv)")
	cmd.Flags().BoolVar(&fromStdin, "stdin", false, "Read db-password from stdin (does not place the secret in argv)")

	return cmd
}

func resolveDBPassword(cmd *cobra.Command, positional, fromFile string, fromStdin bool) (string, error) {
	switch {
	case fromFile != "":
		return util.ReadSecretFile(fromFile)
	case fromStdin:
		return util.ReadSecret(cmd.InOrStdin(), cmd.OutOrStdout(), "")
	case positional != "":
		if _, err := fmt.Fprintln(cmd.ErrOrStderr(), util.DeprecatedPasswordArgWarning); err != nil {
			return "", err
		}
		return positional, nil
	}
	if envVal := strings.TrimRight(os.Getenv(util.DBPasswordEnvVar), "\r\n"); envVal != "" {
		return envVal, nil
	}
	in := cmd.InOrStdin()
	prompt := ""
	if util.IsSecretInputTerminal(in) {
		prompt = "Enter db-password: "
	}
	return util.ReadSecret(in, cmd.OutOrStdout(), prompt)
}

func settingValue(settings *config.Settings, key string) string {
	switch key {
	case "user":
		return settings.User
	case "base-dir":
		return settings.BaseDir
	case "db-type":
		return settings.DBType
	case "db-url":
		return settings.DBURL
	case "db-password":
		return settings.DBPassword
	default:
		return ""
	}
}
