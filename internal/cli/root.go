package cli

import (
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/cli/env"
	"github.com/danieljhkim/local-data-platform/internal/cli/profile"
	"github.com/danieljhkim/local-data-platform/internal/cli/service"
	"github.com/danieljhkim/local-data-platform/internal/cli/wrappers"
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

var (
	// Global paths instance
	paths *config.Paths
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "local-data",
	Short: "Manage a local Hadoop (HDFS + YARN) + Hive stack",
	Long: `local-data: manage a local Hadoop (HDFS + YARN) + Hive stack.

A modular CLI to manage HDFS/YARN/Hive/Spark in one place with runtime
config overlays and profile-based configuration.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// Add subcommands
	rootCmd.AddCommand(profile.NewProfileCmd(getPaths))
	rootCmd.AddCommand(env.NewEnvCmd(getPaths))
	rootCmd.AddCommand(service.NewStartCmd(getPaths))
	rootCmd.AddCommand(service.NewStopCmd(getPaths))
	rootCmd.AddCommand(service.NewStatusCmd(getPaths))
	rootCmd.AddCommand(NewLogsCmd(getPaths))

	// Add wrapper commands
	rootCmd.AddCommand(wrappers.NewHDFSCmd(getPaths))
	rootCmd.AddCommand(wrappers.NewHiveCmd(getPaths))
	rootCmd.AddCommand(wrappers.NewYARNCmd(getPaths))
	rootCmd.AddCommand(wrappers.NewHadoopCmd(getPaths))
	rootCmd.AddCommand(wrappers.NewPySparkCmd(getPaths))
	rootCmd.AddCommand(wrappers.NewSparkSubmitCmd(getPaths))
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
