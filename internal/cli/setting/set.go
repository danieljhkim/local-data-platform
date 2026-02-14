package setting

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/spf13/cobra"
)

func newSetCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a configurable user setting",
		Long: `Set a configurable user setting.

Supported keys: user, db-type, db-url, db-password.
Note: base-dir is static and cannot be changed via this command.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			value := args[1]
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
					fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: db-url %q does not match db-type %q; resetting db-url to default.\n", settings.DBURL, settings.DBType)
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
				fmt.Fprintf(cmd.ErrOrStderr(), "WARNING: %v\n", err)
				return fmt.Errorf("db-type and db-url must match")
			}

			if err := sm.Save(settings); err != nil {
				return err
			}

			applier := config.NewSettingsApplier(paths)
			if err := applier.Apply(key, oldValue, value); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "Updated %s in %s\n", key, sm.Path())
			fmt.Fprintln(cmd.ErrOrStderr(), "WARNING: Run 'local-data init --force' to ensure regenerated profiles fully reflect updated settings.")
			return nil
		},
	}

	return cmd
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
