package setting

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

// PathsGetter is a function that returns the Paths instance.
type PathsGetter func() *config.Paths

// NewSettingCmd creates the setting command with all subcommands.
func NewSettingCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "setting",
		Short: "Manage user settings",
		Long: `Manage user settings for local-data-platform.

Settings are persisted at $BASE_DIR/settings/setting.json.`,
	}

	cmd.AddCommand(newListCmd(pathsGetter))
	cmd.AddCommand(newSetCmd(pathsGetter))
	cmd.AddCommand(newShowCmd(pathsGetter))

	return cmd
}
