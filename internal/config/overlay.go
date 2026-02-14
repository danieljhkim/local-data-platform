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
	sm := NewSettingsManager(pm.paths)

	effectiveOpts, settingsToPersist, err := pm.resolveInitOptions(sm, opts)
	if err != nil {
		return err
	}

	// Check if destination already exists
	if util.DirExists(dst) {
		if force {
			util.Log("Re-initializing profiles (overwriting): %s", dst)
			if err := os.RemoveAll(dst); err != nil {
				return fmt.Errorf("failed to remove existing profiles: %w", err)
			}
		} else {
			// Keep existing files, but sync mutable settings into generated Hive XML.
			applier := NewSettingsApplier(pm.paths)
			if err := applier.Apply("user", "", effectiveOpts.User); err != nil {
				return fmt.Errorf("failed to sync user setting: %w", err)
			}
			if err := applier.Apply("db-url", "", effectiveOpts.DBUrl); err != nil {
				return fmt.Errorf("failed to sync db-url setting: %w", err)
			}
			if err := applier.Apply("db-password", "", effectiveOpts.DBPassword); err != nil {
				return fmt.Errorf("failed to sync db-password setting: %w", err)
			}
			if err := sm.Save(settingsToPersist); err != nil {
				return fmt.Errorf("failed to save settings: %w", err)
			}
			return nil
		}
	}

	// Ensure parent directory exists
	if err := util.MkdirAll(filepath.Dir(dst)); err != nil {
		return err
	}

	util.Log("Generating profiles under: %s", dst)

	gen := generator.NewConfigGenerator()
	if err := gen.InitProfiles(pm.paths.BaseDir, dst, effectiveOpts); err != nil {
		return fmt.Errorf("failed to generate profiles: %w", err)
	}

	if err := sm.Save(settingsToPersist); err != nil {
		return fmt.Errorf("failed to save settings: %w", err)
	}

	util.Log("Profiles initialized successfully")
	util.Log("  Runtime config overlay: %s", pm.paths.CurrentConfDir())

	return nil
}

func (pm *ProfileManager) resolveInitOptions(sm *SettingsManager, opts *generator.InitOptions) (*generator.InitOptions, *Settings, error) {
	settings, err := sm.LoadOrDefault()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load settings: %w", err)
	}

	effective := &generator.InitOptions{
		User:       settings.User,
		DBUrl:      settings.DBURL,
		DBPassword: settings.DBPassword,
	}

	if opts != nil {
		if opts.User != "" {
			effective.User = opts.User
		}
		if opts.DBUrl != "" {
			effective.DBUrl = opts.DBUrl
		}
		if opts.DBPassword != "" {
			effective.DBPassword = opts.DBPassword
		}
	}

	persisted := &Settings{
		User:       effective.User,
		BaseDir:    pm.paths.BaseDir,
		DBURL:      effective.DBUrl,
		DBPassword: effective.DBPassword,
	}

	return effective, persisted, nil
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
	srcRoot := filepath.Join(pm.paths.UserProfilesDir(), profile)

	// Check if profile exists in user's profiles directory
	if !util.DirExists(srcRoot) {
		return fmt.Errorf("profile '%s' not found in %s (run: local-data profile init)", profile, pm.paths.UserProfilesDir())
	}

	util.Log("Applying runtime config overlay for profile '%s'", profile)
	util.Log("  to: %s", dstRoot)

	// Remove existing overlay to ensure clean state
	if util.DirExists(dstRoot) {
		if err := os.RemoveAll(dstRoot); err != nil {
			return fmt.Errorf("failed to remove existing overlay: %w", err)
		}
	}

	// Copy profile configs from user's profile directory to current overlay
	// This preserves customizations made during 'profile init'
	if err := util.CopyDir(srcRoot, dstRoot); err != nil {
		return fmt.Errorf("failed to copy profile configs: %w", err)
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
