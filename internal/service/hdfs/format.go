package hdfs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// EnsureNameNodeFormatted checks if NameNode is formatted and formats it if needed
// Mirrors ld_hdfs_ensure_namenode_formatted
func EnsureNameNodeFormatted(hadoopConfDir string) error {
	// Parse namenode directories from hdfs-site.xml
	hdfsConf := filepath.Join(hadoopConfDir, "hdfs-site.xml")
	dirs, err := util.ParseNameNodeDirs(hdfsConf)
	if err != nil {
		// If we can't parse the config, skip formatting
		// This might be expected for non-HDFS profiles
		return nil
	}

	if len(dirs) == 0 {
		return nil // No directories configured
	}

	// Check if already formatted by looking for VERSION file
	// Don't rely solely on PID check as a process might be running but failing
	alreadyFormatted := false
	for _, dir := range dirs {
		versionFile := filepath.Join(dir, "current", "VERSION")
		if util.FileExists(versionFile) {
			alreadyFormatted = true
			break
		}
	}

	// If already formatted, skip formatting even if no process is running
	if alreadyFormatted {
		return nil
	}

	// If a NameNode is currently running but not formatted, this indicates
	// a serious problem - don't try to format while it's running
	pid, _ := FindNameNodePID()
	if pid != 0 {
		return fmt.Errorf("NameNode process is running (pid %d) but directory is not formatted.\n"+
			"  This indicates a serious issue. Stop the NameNode and try again:\n"+
			"    local-data stop hdfs", pid)
	}

	// Check each directory
	for _, dir := range dirs {
		versionFile := filepath.Join(dir, "current", "VERSION")

		// Check if already formatted
		if util.FileExists(versionFile) {
			// Directory is formatted, we're done
			continue
		}

		// Check if directory is empty
		isEmpty, err := util.IsDirEmpty(dir)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to check if directory is empty: %w", err)
		}

		if os.IsNotExist(err) || isEmpty {
			// Directory doesn't exist or is empty, safe to format
			util.Log("Formatting NameNode (first time)")
			if err := formatNameNode(hadoopConfDir); err != nil {
				return fmt.Errorf("failed to format NameNode: %w", err)
			}

			// Verify formatting succeeded
			if !util.FileExists(versionFile) {
				return fmt.Errorf("NameNode format completed but VERSION file not created: %s\n"+
					"  This may indicate HADOOP_CONF_DIR is not set correctly or Hadoop installation is corrupted.", versionFile)
			}
			util.Log("NameNode formatted successfully")
			continue
		}

		// Directory exists, is not empty, but has no VERSION file
		// This is unsafe - refuse to format
		return fmt.Errorf("NameNode directory exists but is not formatted: %s\n"+
			"  This may indicate a corrupted installation or wrong configuration.\n"+
			"  To format anyway, manually delete the directory and try again:\n"+
			"    rm -rf %s", dir, dir)
	}

	return nil
}

// formatNameNode runs the HDFS namenode format command
func formatNameNode(hadoopConfDir string) error {
	cmd := exec.Command("hdfs", "namenode", "-format", "-force", "-nonInteractive")

	// Set HADOOP_CONF_DIR so format uses the correct configuration
	cmd.Env = append(os.Environ(), "HADOOP_CONF_DIR="+hadoopConfDir)

	// Capture output to show on error
	output, err := cmd.CombinedOutput()

	if err != nil {
		// Show output to help diagnose the issue
		if len(output) > 0 {
			util.Warn("Format command output:\n%s", string(output))
		}
		return fmt.Errorf("failed to format NameNode: %w", err)
	}

	return nil
}

// EnsureLocalStorageDirs creates the local filesystem directories needed by HDFS
// Mirrors ld_hdfs_ensure_local_storage_dirs
func EnsureLocalStorageDirs(baseDir string) error {
	dirs := []string{
		filepath.Join(baseDir, "state", "hdfs", "namenode"),
		filepath.Join(baseDir, "state", "hdfs", "datanode"),
		filepath.Join(baseDir, "state", "hadoop", "tmp"),
	}

	return util.MkdirAll(dirs...)
}

// CreateCommonHDFSDirs creates common HDFS directories after startup
// Creates /tmp, /user/<username>, /user/hive/warehouse, /spark-history
func CreateCommonHDFSDirs(username string) error {
	return CreateCommonHDFSDirsWithEnv(username, nil)
}

// CreateCommonHDFSDirsWithEnv creates common HDFS directories with custom environment
func CreateCommonHDFSDirsWithEnv(username string, env []string) error {
	// Create directories
	dirs := []struct {
		path string
		perm string // permissions to set
	}{
		{"/tmp", "1777"},                       // sticky bit
		{"/user/" + username, ""},              // default perms
		{"/user/hive/warehouse", "g+w"},        // group writable
		{"/spark-history", "1777"},             // sticky bit
	}

	for _, dir := range dirs {
		// Create directory
		cmd := exec.Command("hdfs", "dfs", "-mkdir", "-p", dir.path)
		if env != nil {
			cmd.Env = env
		}
		if err := cmd.Run(); err != nil {
			// Log warning but don't fail - directory might already exist
			util.Warn("Failed to create HDFS directory %s: %v", dir.path, err)
			continue
		}

		// Set permissions if specified
		if dir.perm != "" {
			cmd = exec.Command("hdfs", "dfs", "-chmod", dir.perm, dir.path)
			if env != nil {
				cmd.Env = env
			}
			if err := cmd.Run(); err != nil {
				util.Warn("Failed to set permissions on %s: %v", dir.path, err)
			}
		}
	}

	return nil
}

// EnsureSparkHistoryDir ensures the /spark-history directory exists in HDFS
// This is called before running Spark commands to ensure the history directory exists
func EnsureSparkHistoryDir(env []string) error {
	// Check if directory exists
	cmd := exec.Command("hdfs", "dfs", "-test", "-d", "/spark-history")
	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err == nil {
		// Directory exists
		return nil
	}

	// Create directory
	util.Log("Creating HDFS /spark-history directory...")
	cmd = exec.Command("hdfs", "dfs", "-mkdir", "-p", "/spark-history")
	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create /spark-history: %w", err)
	}

	// Set permissions
	cmd = exec.Command("hdfs", "dfs", "-chmod", "1777", "/spark-history")
	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		util.Warn("Failed to set permissions on /spark-history: %v", err)
	}

	return nil
}
