package setting

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
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
			fmt.Fprintf(out, "user=%s\n", settings.User)
			fmt.Fprintf(out, "base-dir=%s\n", settings.BaseDir)
			fmt.Fprintf(out, "db-type=%s\n", settings.DBType)
			fmt.Fprintf(out, "db-url=%s\n", settings.DBURL)
			fmt.Fprintf(out, "db-password=%s\n", maskedPassword(settings.DBPassword))
			return nil
		},
	}

	return cmd
}

func maskedPassword(value string) string {
	if value == "" {
		return ""
	}
	return "********"
}
