package setting

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

func newListCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List configurable user settings",
		Long:  `List all configurable user settings and current values.`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			sm := config.NewSettingsManager(pathsGetter())
			settings, err := sm.LoadOrDefault()
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if _, err := fmt.Fprintf(out, "  - user: %s\n  - base-dir: %s\n  - db-type: %s\n  - db-url: %s\n  - db-password: %s\n",
				settings.User,
				settings.BaseDir,
				settings.DBType,
				util.RedactJDBCURL(settings.DBURL),
				maskedPassword(settings.DBPassword),
			); err != nil {
				return err
			}
			return nil
		},
	}

	return cmd
}

func maskedPassword(value string) string {
	if value == "" {
		return ""
	}
	return util.RedactedValue
}
