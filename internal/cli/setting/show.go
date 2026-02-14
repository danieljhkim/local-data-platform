package setting

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

func newShowCmd(pathsGetter PathsGetter) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <hadoop|hive|spark>",
		Short: "Show active profile configuration file contents",
		Long:  `Show the active runtime configuration contents from $BASE_DIR/conf/current/.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := pathsGetter()

			var files []string
			switch args[0] {
			case "hadoop":
				var err error
				files, err = collectHadoopFiles(paths)
				if err != nil {
					return err
				}
			case "hive":
				files = []string{filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")}
			case "spark":
				files = []string{
					filepath.Join(paths.CurrentSparkConf(), "spark-defaults.conf"),
					filepath.Join(paths.CurrentSparkConf(), "hive-site.xml"),
				}
			default:
				return fmt.Errorf("unknown target %q (supported: hadoop, hive, spark)", args[0])
			}

			return printFiles(cmd.OutOrStdout(), files, args[0] == "spark")
		},
	}

	return cmd
}

func collectHadoopFiles(paths *config.Paths) ([]string, error) {
	confDir := paths.CurrentHadoopConf()
	if !util.DirExists(confDir) {
		return nil, fmt.Errorf("active profile has no hadoop config at %s", confDir)
	}

	entries, err := os.ReadDir(confDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read hadoop config dir: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".xml") {
			files = append(files, filepath.Join(confDir, entry.Name()))
		}
	}
	sort.Strings(files)

	if len(files) == 0 {
		return nil, fmt.Errorf("no hadoop XML files found in %s", confDir)
	}

	return files, nil
}

func printFiles(out interface {
	Write([]byte) (int, error)
}, files []string, optionalMissing bool) error {
	printed := 0

	for _, file := range files {
		if !util.FileExists(file) {
			if optionalMissing {
				continue
			}
			return fmt.Errorf("config file not found: %s", file)
		}

		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", file, err)
		}

		if _, err := fmt.Fprintf(out, "=== %s ===\n", file); err != nil {
			return err
		}
		if _, err := out.Write(data); err != nil {
			return err
		}
		if len(data) == 0 || data[len(data)-1] != '\n' {
			if _, err := out.Write([]byte("\n")); err != nil {
				return err
			}
		}
		printed++
	}

	if printed == 0 {
		return fmt.Errorf("no config files found to display")
	}

	return nil
}
