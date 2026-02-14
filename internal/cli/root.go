package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/cli/env"
	"github.com/danieljhkim/local-data-platform/internal/cli/profile"
	"github.com/danieljhkim/local-data-platform/internal/cli/service"
	"github.com/danieljhkim/local-data-platform/internal/cli/setting"
	"github.com/danieljhkim/local-data-platform/internal/cli/wrappers"
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
	"github.com/spf13/cobra"
)

var (
	// Global paths instance
	paths *config.Paths
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "local-data",
	Version: "dev",
	Short:   "Manage a local Hadoop (HDFS + YARN) + Hive stack",
	Long: `local-data: manage a local Hadoop (HDFS + YARN) + Hive stack.

A modular CLI to manage HDFS/YARN/Hive/Spark in one place with runtime
config overlays and profile-based configuration.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

func SetVersion(v string) {
	if v == "" {
		return
	}
	rootCmd.Version = v
	rootCmd.SetVersionTemplate("{{.Version}}\n")
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

// colorizeHelp applies color to section headers in Cobra help output.
// Headers like "Usage:", "Cluster Management:", "Flags:" are bolded/colored.
func colorizeHelp(s string) string {
	if !util.StdoutColorEnabled() {
		return s
	}

	lines := strings.Split(s, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Section headers end with ":" and have no leading whitespace (or are group titles)
		if trimmed == "" || strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			continue
		}
		if strings.HasSuffix(trimmed, ":") {
			lines[i] = util.Colorf(util.BoldCyan, "%s", line)
		}
	}
	return strings.Join(lines, "\n")
}

func init() {
	cobra.OnInitialize(initConfig)

	// Custom help function to colorize section headers
	defaultHelp := rootCmd.HelpFunc()
	rootCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		// Capture the default help output
		buf := new(strings.Builder)
		cmd.SetOut(buf)
		defaultHelp(cmd, args)
		cmd.SetOut(os.Stdout)
		fmt.Print(colorizeHelp(buf.String()))
	})

	// Define command groups
	rootCmd.AddGroup(
		&cobra.Group{ID: "cluster", Title: "Cluster Management:"},
		&cobra.Group{ID: "platform", Title: "Data Platform Commands:"},
		&cobra.Group{ID: "config", Title: "Configuration:"},
		&cobra.Group{ID: "util", Title: "CLI Utilities:"},
	)

	// Cluster Management
	addCmdToGroup(rootCmd, newInitCmd(getPaths), "cluster")
	addCmdToGroup(rootCmd, service.NewStartCmd(getPaths), "cluster")
	addCmdToGroup(rootCmd, service.NewStopCmd(getPaths), "cluster")
	addCmdToGroup(rootCmd, service.NewStatusCmd(getPaths), "cluster")
	addCmdToGroup(rootCmd, NewLogsCmd(getPaths), "cluster")

	// Data Platform Commands
	addCmdToGroup(rootCmd, wrappers.NewHadoopCmd(getPaths), "platform")
	addCmdToGroup(rootCmd, wrappers.NewHDFSCmd(getPaths), "platform")
	addCmdToGroup(rootCmd, wrappers.NewHiveCmd(getPaths), "platform")
	addCmdToGroup(rootCmd, wrappers.NewPySparkCmd(getPaths), "platform")
	addCmdToGroup(rootCmd, wrappers.NewSparkSubmitCmd(getPaths), "platform")
	addCmdToGroup(rootCmd, wrappers.NewYARNCmd(getPaths), "platform")

	// Configuration
	addCmdToGroup(rootCmd, profile.NewProfileCmd(getPaths), "config")
	addCmdToGroup(rootCmd, env.NewEnvCmd(getPaths), "config")
	addCmdToGroup(rootCmd, setting.NewSettingCmd(getPaths), "config")

	// CLI Utilities
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the local-data CLI version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(os.Stdout, rootCmd.Version)
		},
	}
	addCmdToGroup(rootCmd, versionCmd, "util")
}

// addCmdToGroup sets the GroupID on a command and adds it to the parent.
func addCmdToGroup(parent *cobra.Command, cmd *cobra.Command, groupID string) {
	cmd.GroupID = groupID
	parent.AddCommand(cmd)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Initialize paths
	repoRoot := getRepoRoot()
	baseDir := config.DefaultBaseDir()
	paths = config.NewPaths(repoRoot, baseDir)
}

// getPaths returns the global paths instance
// This is passed to subcommands as a getter function
func getPaths() *config.Paths {
	if paths == nil {
		initConfig()
	}
	return paths
}

// getRepoRoot determines the repository root directory
// Looks for conf/ directory next to the binary or one level up
// Returns empty string if not found (repo root is optional with generator-based profiles)
func getRepoRoot() string {
	// Get the executable path
	exe, err := os.Executable()
	if err != nil {
		return ""
	}

	// Get the directory containing the executable
	exeDir := filepath.Dir(exe)

	// Check if conf/ is next to the executable (when in repo/bin/)
	if fileExists(filepath.Join(exeDir, "conf")) {
		return exeDir
	}

	// Check if conf/ is one level up (when binary is in bin/)
	parent := filepath.Dir(exeDir)
	if fileExists(filepath.Join(parent, "conf")) {
		return parent
	}

	// Fallback: use current working directory
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}

	// Check if conf/ is in current directory
	if fileExists(filepath.Join(cwd, "conf")) {
		return cwd
	}

	// Check if conf/ is one level up from current directory
	parent = filepath.Dir(cwd)
	if fileExists(filepath.Join(parent, "conf")) {
		return parent
	}

	// Repo root not found - this is OK since profiles are now generated
	return ""
}

// fileExists checks if a file or directory exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
