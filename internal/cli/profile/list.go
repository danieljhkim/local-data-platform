package profile

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newListCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available profiles",
		Long:  `List all available configuration profiles.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			profiles, err := pm.List()
			if err != nil {
				return err
			}

			for _, profile := range profiles {
				fmt.Println(profile)
			}

			return nil
		},
	}

	return cmd
}
