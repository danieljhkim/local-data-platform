package config

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// Paths holds all standard path locations for the local-data-platform
// Mirrors the path computation functions from lib/local_data/common.sh
type Paths struct {
	RepoRoot string // Repository root directory
	BaseDir  string // Base directory for runtime state ($BASE_DIR)
}

// NewPaths creates a new Paths instance
// repoRoot: path to the repository root
// baseDir: base directory (empty string uses default)
func NewPaths(repoRoot, baseDir string) *Paths {
	if baseDir == "" {
		baseDir = DefaultBaseDir()
	}
	return &Paths{
		RepoRoot: repoRoot,
		BaseDir:  baseDir,
	}
}

// DefaultBaseDir returns the default base directory
// Mirrors ld_default_base_dir: ${BASE_DIR:-$HOME/local-data-platform}
func DefaultBaseDir() string {
	home := os.Getenv("HOME")
	if home == "" {
		// Fallback to user.Current if HOME not set
		if currentUser, err := user.Current(); err == nil {
			home = currentUser.HomeDir
		}
	}

	return filepath.Join(home, "local-data-platform")
}

// StateDir returns the state directory: $BASE_DIR/state
// Mirrors ld_state_dir
func (p *Paths) StateDir() string {
	return filepath.Join(p.BaseDir, "state")
}

// SettingsDir returns the settings directory: $BASE_DIR/settings
func (p *Paths) SettingsDir() string {
	return filepath.Join(p.BaseDir, "settings")
}

// SettingsFile returns the settings file path: $BASE_DIR/settings/setting.json
func (p *Paths) SettingsFile() string {
	return filepath.Join(p.SettingsDir(), "setting.json")
}

// ConfRootDir returns the configuration root directory: $BASE_DIR/conf
// Mirrors ld_conf_root_dir
func (p *Paths) ConfRootDir() string {
	return filepath.Join(p.BaseDir, "conf")
}

// CurrentConfDir returns the current runtime config overlay directory
// $BASE_DIR/conf/current
// Mirrors ld_current_conf_dir
func (p *Paths) CurrentConfDir() string {
	return filepath.Join(p.ConfRootDir(), "current")
}

// ActiveProfileFile returns the path to the active profile marker file
// $BASE_DIR/conf/active_profile
// Mirrors ld_active_profile_file
func (p *Paths) ActiveProfileFile() string {
	return filepath.Join(p.ConfRootDir(), "active_profile")
}

// ProfilesDir returns the profiles directory
// Prefers user-initialized profiles over repo profiles
// $BASE_DIR/conf/profiles (if exists) or $REPO_ROOT/conf/profiles
// Mirrors ld_profiles_dir
func (p *Paths) ProfilesDir() string {
	userProfiles := filepath.Join(p.ConfRootDir(), "profiles")
	if util.DirExists(userProfiles) {
		return userProfiles
	}
	return filepath.Join(p.RepoRoot, "conf", "profiles")
}

// RepoProfilesDir returns the repository's profiles directory
// $REPO_ROOT/conf/profiles
func (p *Paths) RepoProfilesDir() string {
	return filepath.Join(p.RepoRoot, "conf", "profiles")
}

// UserProfilesDir returns the user's local profiles directory
// $BASE_DIR/conf/profiles
func (p *Paths) UserProfilesDir() string {
	return filepath.Join(p.ConfRootDir(), "profiles")
}

// CurrentHadoopConf returns the current Hadoop configuration directory
// $BASE_DIR/conf/current/hadoop
func (p *Paths) CurrentHadoopConf() string {
	return filepath.Join(p.CurrentConfDir(), "hadoop")
}

// CurrentHiveConf returns the current Hive configuration directory
// $BASE_DIR/conf/current/hive
func (p *Paths) CurrentHiveConf() string {
	return filepath.Join(p.CurrentConfDir(), "hive")
}

// CurrentSparkConf returns the current Spark configuration directory
// $BASE_DIR/conf/current/spark
func (p *Paths) CurrentSparkConf() string {
	return filepath.Join(p.CurrentConfDir(), "spark")
}

// ServicePaths holds paths for a specific service
type ServicePaths struct {
	StateDir string // Service state directory
	LogsDir  string // Service logs directory
	PidsDir  string // Service PID files directory
	DataDir  string // Service data directory (optional, service-specific)
}

// ServiceStateDir returns paths for a specific service
// service: "hdfs", "yarn", or "hive"
func (p *Paths) ServiceStateDir(service string) *ServicePaths {
	baseStateDir := filepath.Join(p.StateDir(), service)
	return &ServicePaths{
		StateDir: baseStateDir,
		LogsDir:  filepath.Join(baseStateDir, "logs"),
		PidsDir:  filepath.Join(baseStateDir, "pids"),
		DataDir:  filepath.Join(baseStateDir, "data"),
	}
}

// HDFSPaths returns HDFS-specific paths
func (p *Paths) HDFSPaths() *ServicePaths {
	return p.ServiceStateDir("hdfs")
}

// YARNPaths returns YARN-specific paths
func (p *Paths) YARNPaths() *ServicePaths {
	return p.ServiceStateDir("yarn")
}

// HivePaths returns Hive-specific paths
func (p *Paths) HivePaths() *ServicePaths {
	sp := p.ServiceStateDir("hive")
	// Hive also has a warehouse directory
	sp.DataDir = filepath.Join(sp.StateDir, "warehouse")
	return sp
}

// HadoopTmpDir returns the Hadoop temporary directory
// $BASE_DIR/state/hadoop/tmp
func (p *Paths) HadoopTmpDir() string {
	return filepath.Join(p.StateDir(), "hadoop", "tmp")
}

// ActiveProfile reads and returns the active profile name
// Returns "local" as default if no profile is set
// Mirrors ld_active_profile
func (p *Paths) ActiveProfile() (string, error) {
	profileFile := p.ActiveProfileFile()

	if !util.FileExists(profileFile) {
		return "local", nil // Default profile
	}

	data, err := os.ReadFile(profileFile)
	if err != nil {
		return "", err
	}

	profile := strings.TrimSpace(string(data))
	if profile == "" {
		return "local", nil
	}

	return profile, nil
}

// SetActiveProfile writes the active profile name to the marker file
func (p *Paths) SetActiveProfile(profile string) error {
	// Ensure conf directory exists
	if err := util.MkdirAll(p.ConfRootDir()); err != nil {
		return err
	}

	profileFile := p.ActiveProfileFile()
	return os.WriteFile(profileFile, []byte(profile), 0644)
}
