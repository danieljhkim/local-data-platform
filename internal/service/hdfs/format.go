package hdfs

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

var (
	findNameNodePIDForFormat = FindNameNodePID
	formatNameNodeForFormat  = formatNameNode
)

type nameNodeStorageState int

const (
	nameNodeStorageFormatted nameNodeStorageState = iota
	nameNodeStorageEmpty
	nameNodeStorageNonEmpty
	nameNodeStorageCorrupt
)

type nameNodeVersion struct {
	storageID     string
	clusterID     string
	namespaceID   string
	creationTime  string
	layoutVersion string
}

func (v nameNodeVersion) identity() string {
	return strings.Join([]string{v.clusterID, v.namespaceID, v.creationTime, v.layoutVersion}, ",")
}

// readNameNodeVersion validates the fields that identify a NameNode storage
// directory. storageID is intentionally not compared across directories: it is
// local to each storage volume, while the namespace identity must agree.
func readNameNodeVersion(path string) (nameNodeVersion, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nameNodeVersion{}, err
	}

	values := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), "=")
		if found {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}

	version := nameNodeVersion{
		storageID:     values["storageID"],
		clusterID:     values["clusterID"],
		namespaceID:   values["namespaceID"],
		creationTime:  values["cTime"],
		layoutVersion: values["layoutVersion"],
	}
	if values["storageType"] != "NAME_NODE" || version.clusterID == "" ||
		version.namespaceID == "" || version.creationTime == "" || version.layoutVersion == "" {
		return nameNodeVersion{}, fmt.Errorf("missing or invalid NameNode storage identity")
	}
	if _, err := strconv.ParseInt(version.namespaceID, 10, 64); err != nil {
		return nameNodeVersion{}, fmt.Errorf("invalid namespaceID: %w", err)
	}
	if _, err := strconv.ParseInt(version.creationTime, 10, 64); err != nil {
		return nameNodeVersion{}, fmt.Errorf("invalid cTime: %w", err)
	}
	if _, err := strconv.ParseInt(version.layoutVersion, 10, 64); err != nil {
		return nameNodeVersion{}, fmt.Errorf("invalid layoutVersion: %w", err)
	}

	return version, nil
}

func inspectNameNodeStorage(dir string) (nameNodeStorageState, nameNodeVersion, error) {
	versionFile := filepath.Join(dir, "current", "VERSION")
	if util.FileExists(versionFile) {
		version, err := readNameNodeVersion(versionFile)
		if err != nil {
			return nameNodeStorageCorrupt, nameNodeVersion{}, fmt.Errorf("invalid VERSION file %s: %w", versionFile, err)
		}
		return nameNodeStorageFormatted, version, nil
	}

	isEmpty, err := util.IsDirEmpty(dir)
	if os.IsNotExist(err) || isEmpty {
		return nameNodeStorageEmpty, nameNodeVersion{}, nil
	}
	if err != nil {
		return nameNodeStorageCorrupt, nameNodeVersion{}, fmt.Errorf("failed to inspect storage directory %s: %w", dir, err)
	}
	return nameNodeStorageNonEmpty, nameNodeVersion{}, nil
}

func verifyFormattedNameNodeStorage(dirs []string) error {
	var expectedIdentity string
	for _, dir := range dirs {
		state, version, err := inspectNameNodeStorage(dir)
		if err != nil {
			return err
		}
		if state != nameNodeStorageFormatted {
			return fmt.Errorf("NameNode format did not create a valid VERSION file for storage directory: %s", dir)
		}
		if expectedIdentity == "" {
			expectedIdentity = version.identity()
			continue
		}
		if version.identity() != expectedIdentity {
			return fmt.Errorf("NameNode storage identity differs across configured directories: %s", strings.Join(dirs, ", "))
		}
	}
	return nil
}

// EnsureNameNodeFormatted checks if NameNode is formatted and formats it if needed
// Mirrors ld_hdfs_ensure_namenode_formatted
func EnsureNameNodeFormatted(hadoopConfDir string) error {
	return EnsureNameNodeFormattedWithEnv(hadoopConfDir, nil)
}

// EnsureNameNodeFormattedWithEnv formats a first-use NameNode using the same
// environment contract as the service processes that will use its storage.
func EnsureNameNodeFormattedWithEnv(hadoopConfDir string, runtimeEnv []string) error {
	// Parse namenode directories from hdfs-site.xml
	hdfsConf := filepath.Join(hadoopConfDir, "hdfs-site.xml")
	dirs, err := util.ParseNameNodeDirs(hdfsConf)
	if err != nil {
		return fmt.Errorf("cannot validate NameNode storage configuration %s: %w", hdfsConf, err)
	}

	if len(dirs) == 0 {
		return fmt.Errorf("cannot validate NameNode storage configuration %s: no dfs.namenode.name.dir paths configured", hdfsConf)
	}

	formatted := 0
	empty := 0
	for _, dir := range dirs {
		state, _, err := inspectNameNodeStorage(dir)
		if err != nil {
			return err
		}
		switch state {
		case nameNodeStorageFormatted:
			formatted++
		case nameNodeStorageEmpty:
			empty++
		case nameNodeStorageNonEmpty:
			return fmt.Errorf("NameNode directory exists but is not formatted: %s\n"+
				"  Refusing to format configured storage directories: %s", dir, strings.Join(dirs, ", "))
		case nameNodeStorageCorrupt:
			return fmt.Errorf("NameNode storage is corrupt: %s", dir)
		}
	}

	if formatted == len(dirs) {
		return verifyFormattedNameNodeStorage(dirs)
	}
	if formatted != 0 || empty != len(dirs) {
		return fmt.Errorf("NameNode storage directories are in a mixed formatted/unformatted state: %s", strings.Join(dirs, ", "))
	}

	// A live NameNode must always win over a first-time format attempt.
	pid, _ := findNameNodePIDForFormat()
	if pid != 0 {
		return fmt.Errorf("NameNode process is running (pid %d); refusing to format storage directories: %s.\n"+
			"  This indicates a serious issue. Stop the NameNode and try again:\n"+
			"    local-data stop hdfs", pid, strings.Join(dirs, ", "))
	}

	util.Log("Formatting NameNode (first time)")
	if err := formatNameNodeForFormat(hadoopConfDir, runtimeEnv); err != nil {
		return fmt.Errorf("failed to format NameNode: %w", err)
	}
	if err := verifyFormattedNameNodeStorage(dirs); err != nil {
		return fmt.Errorf("NameNode format completed but storage verification failed: %w", err)
	}
	util.Success("NameNode formatted successfully")
	return nil
}

// formatNameNode runs the HDFS namenode format command
func formatNameNode(hadoopConfDir string, runtimeEnv []string) error {
	cmd := exec.Command("hdfs", "namenode", "-format", "-force", "-nonInteractive")

	cmd.Env = withHadoopConfDir(runtimeEnv, hadoopConfDir)

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

func withHadoopConfDir(runtimeEnv []string, hadoopConfDir string) []string {
	if runtimeEnv == nil {
		runtimeEnv = os.Environ()
	}
	result := make([]string, 0, len(runtimeEnv)+1)
	for _, entry := range runtimeEnv {
		if !strings.HasPrefix(entry, "HADOOP_CONF_DIR=") {
			result = append(result, entry)
		}
	}
	return append(result, "HADOOP_CONF_DIR="+hadoopConfDir)
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
		{"/tmp", "1777"},                // sticky bit
		{"/user/" + username, ""},       // default perms
		{"/user/hive/warehouse", "g+w"}, // group writable
		{"/spark-history", "1777"},      // sticky bit
	}

	for _, dir := range dirs {
		// Create directory
		cmd := exec.Command("hdfs", "dfs", "-mkdir", "-p", dir.path)
		if env != nil {
			cmd.Env = env
		}
		if err := cmd.Run(); err != nil {
			return hdfsControlCommandError("hdfs dfs -mkdir -p "+dir.path, env, err)
		}

		// Set permissions if specified
		if dir.perm != "" {
			cmd = exec.Command("hdfs", "dfs", "-chmod", dir.perm, dir.path)
			if env != nil {
				cmd.Env = env
			}
			if err := cmd.Run(); err != nil {
				return hdfsControlCommandError("hdfs dfs -chmod "+dir.perm+" "+dir.path, env, err)
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
		return hdfsControlCommandError("hdfs dfs -mkdir -p /spark-history", env, err)
	}

	// Set permissions
	cmd = exec.Command("hdfs", "dfs", "-chmod", "1777", "/spark-history")
	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		return hdfsControlCommandError("hdfs dfs -chmod 1777 /spark-history", env, err)
	}

	return nil
}
