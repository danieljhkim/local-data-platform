package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// ProfileManager handles profile initialization, listing, setting, and overlay application
type ProfileManager struct {
	paths *Paths
}

// NewProfileManager creates a new profile manager
func NewProfileManager(paths *Paths) *ProfileManager {
	return &ProfileManager{
		paths: paths,
	}
}

// IsInitialized checks if profiles have been initialized
func (pm *ProfileManager) IsInitialized() bool {
	return util.DirExists(pm.paths.UserProfilesDir())
}

// Init initializes profiles using the Go struct generator
func (pm *ProfileManager) Init(force bool, opts *generator.InitOptions) error {
	dst := pm.paths.UserProfilesDir()

	// Check if destination already exists
	if util.DirExists(dst) {
		if force {
			util.Log("Re-initializing profiles (overwriting): %s", dst)
			if err := os.RemoveAll(dst); err != nil {
				return fmt.Errorf("failed to remove existing profiles: %w", err)
			}
		} else {
			util.Log("Profiles already initialized: %s", dst)
			util.Log("  (use: local-data profile init --force to overwrite)")
			return nil
		}
	}

	// Ensure parent directory exists
	if err := util.MkdirAll(filepath.Dir(dst)); err != nil {
		return err
	}

	util.Log("Generating profiles under: %s", dst)

	gen := generator.NewConfigGenerator()
	if err := gen.InitProfiles(pm.paths.BaseDir, dst, opts); err != nil {
		return fmt.Errorf("failed to generate profiles: %w", err)
	}

	util.Log("Profiles initialized successfully")
	util.Log("  Runtime config overlay: %s", pm.paths.CurrentConfDir())

	return nil
}

// List returns a sorted list of available profile names
func (pm *ProfileManager) List() ([]string, error) {
	pdir := pm.paths.UserProfilesDir()

	if !util.DirExists(pdir) {
		return nil, fmt.Errorf("profiles not initialized. Run: local-data profile init")
	}

	// Read directory entries
	entries, err := os.ReadDir(pdir)
	if err != nil {
		return nil, fmt.Errorf("failed to read profiles directory: %w", err)
	}

	var profiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			profiles = append(profiles, entry.Name())
		}
	}

	sort.Strings(profiles)
	return profiles, nil
}

// Set sets the active profile and applies the runtime config overlay
func (pm *ProfileManager) Set(profile string) error {
	if profile == "" {
		return fmt.Errorf("profile name required")
	}

	pdir := pm.paths.UserProfilesDir()
	profilePath := filepath.Join(pdir, profile)
	if !util.DirExists(profilePath) {
		return fmt.Errorf("unknown profile '%s' (expected: %s)", profile, profilePath)
	}

	// Ensure conf root exists
	if err := util.MkdirAll(pm.paths.ConfRootDir()); err != nil {
		return err
	}

	// Write active profile marker
	if err := pm.paths.SetActiveProfile(profile); err != nil {
		return err
	}

	util.Log("Active profile set: %s", profile)

	// Apply the overlay
	return pm.Apply(profile)
}

// Apply applies the runtime config overlay for a profile
func (pm *ProfileManager) Apply(profile string) error {
	// If profile is empty, use active profile
	if profile == "" {
		var err error
		profile, err = pm.paths.ActiveProfile()
		if err != nil {
			return err
		}
	}

	dstRoot := pm.paths.CurrentConfDir()

	gen := generator.NewConfigGenerator()
	if !gen.HasProfile(profile) {
		return fmt.Errorf("unknown profile '%s'", profile)
	}

	util.Log("Applying runtime config overlay for profile '%s'", profile)
	util.Log("  to: %s", dstRoot)

	// Generate config files programmatically
	if err := gen.Generate(profile, pm.paths.BaseDir, dstRoot); err != nil {
		return fmt.Errorf("failed to generate config: %w", err)
	}

	// Copy hive-site.xml into Spark conf so PySpark/spark-submit find the metastore
	hiveConfig := filepath.Join(dstRoot, "hive", "hive-site.xml")
	if util.FileExists(hiveConfig) {
		sparkHiveConfig := filepath.Join(dstRoot, "spark", "hive-site.xml")
		if err := util.CopyFile(hiveConfig, sparkHiveConfig); err != nil {
			util.Warn("Failed to copy hive-site.xml to Spark conf: %v", err)
		}
	}

	// Write marker file
	markerPath := filepath.Join(dstRoot, ".profile")
	if err := os.WriteFile(markerPath, []byte(profile), 0644); err != nil {
		return fmt.Errorf("failed to write profile marker: %w", err)
	}

	return nil
}

// Check verifies that the runtime config overlay exists and is valid
func (pm *ProfileManager) Check() error {
	cur := pm.paths.CurrentConfDir()

	if !util.DirExists(cur) {
		return fmt.Errorf("runtime conf overlay not found. Run: local-data profile set <name>")
	}

	// Hadoop configs are optional (e.g. 'local' profile doesn't use Hadoop)
	hadoopConf := filepath.Join(cur, "hadoop")
	if util.DirExists(hadoopConf) {
		requiredConfigs := []string{
			"core-site.xml",
			"hdfs-site.xml",
			"mapred-site.xml",
			"yarn-site.xml",
		}

		for _, f := range requiredConfigs {
			configPath := filepath.Join(hadoopConf, f)
			if !util.FileExists(configPath) {
				return fmt.Errorf("missing runtime Hadoop config: %s", configPath)
			}
		}
	}

	// Hive config is required
	hiveConfig := filepath.Join(cur, "hive", "hive-site.xml")
	if !util.FileExists(hiveConfig) {
		return fmt.Errorf("missing runtime Hive config: %s", hiveConfig)
	}

	util.Log("OK: runtime config overlay present at %s", cur)
	return nil
}
