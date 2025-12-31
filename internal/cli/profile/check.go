package profile

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newCheckCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Verify required config files exist in the runtime overlay",
		Long: `Check that the runtime configuration overlay is present and valid.

Verifies that all required configuration files exist in $BASE_DIR/conf/current/.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)
			return pm.Check()
		},
	}

	return cmd
}
