package profile

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

func newDiffCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff <profile-name>",
		Short: "Preview overlay changes for a profile without activating it",
		Long: `Compare the current runtime overlay with the overlay profile set would activate.

The preview materializes the candidate in an isolated temporary directory using
the same overlay construction as profile set. It does not change the active
profile marker, runtime overlay, settings, or running services.

Password and credential values are redacted in the output.

Examples:
  local-data profile diff hdfs
  local-data profile diff local`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			profileName := args[0]
			paths := pathsGetter()
			pm := config.NewProfileManager(paths)

			if !pm.IsInitialized() {
				return fmt.Errorf("profiles have not been initialized\n\nRun: local-data init")
			}

			profiles, err := pm.List()
			if err != nil {
				return fmt.Errorf("failed to list profiles: %w", err)
			}

			found := false
			for _, p := range profiles {
				if p == profileName {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("unknown profile '%s'\n\nAvailable profiles: %v\nRun: local-data profile list", profileName, profiles)
			}

			diff, err := pm.Diff(profileName)
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(cmd.OutOrStdout(), diff.Format())
			return err
		},
	}

	return cmd
}
