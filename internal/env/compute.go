package env

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// Environment holds all computed environment variables
// Mirrors the environment computation from ld_env_print
type Environment struct {
	BaseDir       string
	RepoRoot      string
	ActiveProfile string

	HadoopHome       string
	HadoopCommonHome string
	HadoopHDFSHome   string
	HadoopMapredHome string
	HadoopYarnHome   string
	HadoopConfDir    string

	HiveHome    string
	HiveConfDir string

	SparkHome    string
	SparkConfDir string

	JavaHome string

	Path string

	// Additional vars
	HiveAuxJarsPath string
}

// Compute computes the complete environment for the active profile
// Mirrors ld_env_print
func Compute(paths *config.Paths) (*Environment, error) {
	// Ensure overlay exists before computing env (keeps wrappers hermetic)
	pm := config.NewProfileManager(paths)
	activeProfile, err := paths.ActiveProfile()
	if err != nil {
		return nil, err
	}

	// Apply overlay silently (no output)
	if err := pm.Apply(activeProfile, false); err != nil {
		return nil, fmt.Errorf("failed to apply profile overlay: %w", err)
	}

	// Detect environment
	detection, err := DetectEnvironment()
	if err != nil {
		return nil, err
	}

	env := &Environment{
		BaseDir:       paths.BaseDir,
		RepoRoot:      paths.RepoRoot,
		ActiveProfile: activeProfile,
		JavaHome:      detection.JavaHome,
	}

	// Hadoop environment (optional - e.g., 'local' profile doesn't use it)
	// Only set Hadoop vars if the profile includes hadoop configuration
	hadoopConfDir := paths.CurrentHadoopConf()
	if util.DirExists(hadoopConfDir) && detection.HadoopHome != "" {
		env.HadoopHome = detection.HadoopHome
		env.HadoopConfDir = hadoopConfDir

		// Set Hadoop-related homes
		if os.Getenv("HADOOP_COMMON_HOME") != "" {
			env.HadoopCommonHome = os.Getenv("HADOOP_COMMON_HOME")
		} else {
			env.HadoopCommonHome = env.HadoopHome
		}

		if os.Getenv("HADOOP_HDFS_HOME") != "" {
			env.HadoopHDFSHome = os.Getenv("HADOOP_HDFS_HOME")
		} else {
			env.HadoopHDFSHome = env.HadoopHome
		}

		if os.Getenv("HADOOP_MAPRED_HOME") != "" {
			env.HadoopMapredHome = os.Getenv("HADOOP_MAPRED_HOME")
		} else {
			env.HadoopMapredHome = env.HadoopHome
		}

		if os.Getenv("HADOOP_YARN_HOME") != "" {
			env.HadoopYarnHome = os.Getenv("HADOOP_YARN_HOME")
		} else {
			env.HadoopYarnHome = env.HadoopHome
		}
	}

	// Hive environment (required)
	env.HiveHome = detection.HiveHome
	env.HiveConfDir = paths.CurrentHiveConf()

	// Spark environment (optional)
	env.SparkHome = detection.SparkHome
	if env.SparkHome != "" {
		env.SparkConfDir = paths.CurrentSparkConf()
	}

	// Build PATH
	env.Path = buildPath(env, paths)

	return env, nil
}

// buildPath constructs the PATH environment variable
// Mirrors the PATH deduplication logic from ld_env_print
func buildPath(env *Environment, paths *config.Paths) string {
	var newParts []string

	// Add repo bin directory
	newParts = append(newParts, filepath.Join(env.RepoRoot, "bin"))

	// Add Java bin
	if env.JavaHome != "" {
		newParts = append(newParts, filepath.Join(env.JavaHome, "bin"))
	}

	// Add Hadoop bin and sbin
	if env.HadoopHome != "" {
		newParts = append(newParts,
			filepath.Join(env.HadoopHome, "bin"),
			filepath.Join(env.HadoopHome, "sbin"),
		)
	}

	// Add Hive bin
	if env.HiveHome != "" {
		newParts = append(newParts, filepath.Join(env.HiveHome, "bin"))
	}

	// Add Spark bin
	if env.SparkHome != "" {
		newParts = append(newParts, filepath.Join(env.SparkHome, "bin"))
	}

	// Get existing PATH
	existingPath := os.Getenv("PATH")

	// Deduplicate
	return util.DeduplicatePath(newParts, existingPath)
}

// Export returns environment variables as []string for exec.Cmd.Env
func (e *Environment) Export() []string {
	var exports []string

	// Helper to add export
	add := func(name, value string) {
		if value != "" {
			exports = append(exports, name+"="+value)
		}
	}

	add("BASE_DIR", e.BaseDir)
	add("REPO_ROOT", e.RepoRoot)
	add("ACTIVE_PROFILE", e.ActiveProfile)

	// Hadoop vars (optional)
	if e.HadoopHome != "" {
		add("HADOOP_HOME", e.HadoopHome)
		add("HADOOP_COMMON_HOME", e.HadoopCommonHome)
		add("HADOOP_HDFS_HOME", e.HadoopHDFSHome)
		add("HADOOP_MAPRED_HOME", e.HadoopMapredHome)
		add("HADOOP_YARN_HOME", e.HadoopYarnHome)
		add("HADOOP_CONF_DIR", e.HadoopConfDir)
	}

	// Hive vars (required)
	add("HIVE_HOME", e.HiveHome)
	add("HIVE_CONF_DIR", e.HiveConfDir)
	if e.HiveAuxJarsPath != "" {
		add("HIVE_AUX_JARS_PATH", e.HiveAuxJarsPath)
	}

	// Spark vars (optional)
	if e.SparkHome != "" {
		add("SPARK_HOME", e.SparkHome)
		add("SPARK_CONF_DIR", e.SparkConfDir)
	}

	// Java
	add("JAVA_HOME", e.JavaHome)

	// PATH
	add("PATH", e.Path)

	return exports
}

// PrintShell prints shell export statements
// Mirrors ld_env_print output format
func (e *Environment) PrintShell() {
	// Helper to emit export statement
	emit := func(name, value string) {
		if value != "" {
			fmt.Printf("export %s=%s\n", name, util.ShellEscape(value))
		}
	}

	emit("BASE_DIR", e.BaseDir)
	emit("REPO_ROOT", e.RepoRoot)
	emit("ACTIVE_PROFILE", e.ActiveProfile)

	// Hadoop vars (optional)
	if e.HadoopHome != "" {
		emit("HADOOP_HOME", e.HadoopHome)
		emit("HADOOP_COMMON_HOME", e.HadoopCommonHome)
		emit("HADOOP_HDFS_HOME", e.HadoopHDFSHome)
		emit("HADOOP_MAPRED_HOME", e.HadoopMapredHome)
		emit("HADOOP_YARN_HOME", e.HadoopYarnHome)
		emit("HADOOP_CONF_DIR", e.HadoopConfDir)
	}

	// Hive vars (required)
	emit("HIVE_HOME", e.HiveHome)
	emit("HIVE_CONF_DIR", e.HiveConfDir)
	if e.HiveAuxJarsPath != "" {
		emit("HIVE_AUX_JARS_PATH", e.HiveAuxJarsPath)
	}

	// Spark vars (optional)
	if e.SparkHome != "" {
		emit("SPARK_HOME", e.SparkHome)
		emit("SPARK_CONF_DIR", e.SparkConfDir)
	}

	// Java
	emit("JAVA_HOME", e.JavaHome)

	// PATH
	emit("PATH", e.Path)
}

// MergeWithCurrent merges this environment with the current process environment
// Returns a complete environment suitable for exec.Cmd.Env
func (e *Environment) MergeWithCurrent() []string {
	// Start with current environment
	current := os.Environ()

	// Create a map for easy lookup and override
	envMap := make(map[string]string)
	for _, entry := range current {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	// Override with our computed environment
	for _, entry := range e.Export() {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	// Convert back to []string
	var result []string
	for key, value := range envMap {
		result = append(result, key+"="+value)
	}

	return result
}
