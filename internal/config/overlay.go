package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
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
	return withConfigLock(pm.paths, func() error {
		dst := pm.paths.UserProfilesDir()
		sm := NewSettingsManager(pm.paths)
		effectiveOpts, settingsToPersist, err := pm.resolveInitOptions(sm, opts)
		if err != nil {
			return err
		}
		if err := util.MkdirAll(filepath.Dir(dst)); err != nil {
			return err
		}

		transactionID := newTransactionID()
		profilesEntry := newStagedReplacement(dst, "profiles", transactionID)
		cleanup := func(entries ...stagedReplacement) {
			for _, entry := range entries {
				_ = os.RemoveAll(entry.Stage)
			}
		}

		if util.DirExists(dst) && !force {
			if err := util.CopyDir(dst, profilesEntry.Stage); err != nil {
				return fmt.Errorf("failed to stage existing profiles: %w", err)
			}
			if err := pm.paths.runConfigHook("profiles.copy"); err != nil {
				cleanup(profilesEntry)
				return err
			}
			if err := applySettingsUnder(profilesEntry.Stage, settingsToPersist); err != nil {
				cleanup(profilesEntry)
				return fmt.Errorf("failed to stage profile settings: %w", err)
			}
		} else {
			if force && util.DirExists(dst) {
				util.Log("Re-initializing profiles (overwriting): %s", dst)
			}
			util.Log("Generating profiles under: %s", dst)
			gen := generator.NewConfigGenerator()
			if err := gen.InitProfiles(pm.paths.BaseDir, profilesEntry.Stage, effectiveOpts); err != nil {
				cleanup(profilesEntry)
				return fmt.Errorf("failed to generate profiles: %w", err)
			}
			if err := pm.paths.runConfigHook("profiles.generate"); err != nil {
				cleanup(profilesEntry)
				return err
			}
		}

		settingsEntry, err := sm.stageSettings(settingsToPersist, transactionID)
		if err != nil {
			cleanup(profilesEntry)
			return fmt.Errorf("failed to stage settings: %w", err)
		}
		entries := []stagedReplacement{profilesEntry}

		// If an overlay is already published, replace it from the staged profile
		// in the same transaction so init never leaves it stale.
		if util.DirExists(pm.paths.CurrentConfDir()) {
			active, err := pm.paths.activeProfileUnlocked()
			if err != nil {
				cleanup(profilesEntry, settingsEntry)
				return err
			}
			overlayEntry, err := pm.stageOverlay(filepath.Join(profilesEntry.Stage, active), active, transactionID)
			if err != nil {
				cleanup(profilesEntry, settingsEntry)
				return err
			}
			entries = append(entries, overlayEntry)
		}
		entries = append(entries, settingsEntry)
		if err := publishStaged(pm.paths, transactionID, entries); err != nil {
			return err
		}

		util.Success("Profiles initialized successfully")
		util.Log("  Runtime config overlay: %s", pm.paths.CurrentConfDir())
		return nil
	})
}

func (pm *ProfileManager) resolveInitOptions(sm *SettingsManager, opts *generator.InitOptions) (*generator.InitOptions, *Settings, error) {
	settings, err := sm.loadOrDefaultUnlocked()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load settings: %w", err)
	}

	effective := &generator.InitOptions{
		User:       settings.User,
		DBType:     settings.DBType,
		DBUrl:      settings.DBURL,
		DBPassword: settings.DBPassword,
	}

	if opts != nil {
		if opts.User != "" {
			effective.User = opts.User
		}
		if opts.DBType != "" {
			effective.DBType = opts.DBType
		}
		if opts.DBUrl != "" {
			effective.DBUrl = opts.DBUrl
		}
		if opts.DBPassword != "" {
			effective.DBPassword = opts.DBPassword
		}
	}

	dbType, err := metastore.NormalizeDBType(effective.DBType)
	if err != nil {
		return nil, nil, err
	}
	effective.DBType = string(dbType)
	if err := metastore.ValidateURL(dbType, effective.DBUrl); err != nil {
		return nil, nil, err
	}

	persisted := &Settings{
		User:       effective.User,
		BaseDir:    pm.paths.BaseDir,
		DBType:     effective.DBType,
		DBURL:      effective.DBUrl,
		DBPassword: effective.DBPassword,
	}

	return effective, persisted, nil
}

// List returns a sorted list of available profile names
func (pm *ProfileManager) List() ([]string, error) {
	var profiles []string
	err := withConfigLock(pm.paths, func() error {
		var err error
		profiles, err = pm.listUnlocked()
		return err
	})
	return profiles, err
}

func (pm *ProfileManager) listUnlocked() ([]string, error) {
	pdir := pm.paths.UserProfilesDir()

	if !util.DirExists(pdir) {
		return nil, fmt.Errorf("profiles not initialized. Run: local-data init")
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
	return withConfigLock(pm.paths, func() error {
		if profile == "" {
			return fmt.Errorf("profile name required")
		}
		profilePath := filepath.Join(pm.paths.UserProfilesDir(), profile)
		if !util.DirExists(profilePath) {
			return fmt.Errorf("unknown profile '%s' (expected: %s)", profile, profilePath)
		}
		if err := util.MkdirAll(pm.paths.ConfRootDir()); err != nil {
			return err
		}

		transactionID := newTransactionID()
		overlayEntry, err := pm.stageOverlay(profilePath, profile, transactionID)
		if err != nil {
			return err
		}
		markerEntry := newStagedReplacement(pm.paths.ActiveProfileFile(), "active-profile", transactionID)
		if err := util.WriteFile(markerEntry.Stage, []byte(profile), util.PublicFileMode); err != nil {
			_ = os.RemoveAll(overlayEntry.Stage)
			_ = os.Remove(markerEntry.Stage)
			return fmt.Errorf("failed to stage active profile: %w", err)
		}
		if err := pm.paths.runConfigHook("active-profile.write"); err != nil {
			_ = os.RemoveAll(overlayEntry.Stage)
			_ = os.Remove(markerEntry.Stage)
			return err
		}

		// The overlay is deliberately first; the marker changes only after a
		// complete replacement is ready, and rollback restores both on error.
		if err := publishStaged(pm.paths, transactionID, []stagedReplacement{overlayEntry, markerEntry}); err != nil {
			return err
		}
		util.Log("Active profile set: %s", profile)
		return nil
	})
}

// Apply applies the runtime config overlay for a profile
func (pm *ProfileManager) Apply(profile string) error {
	return withConfigLock(pm.paths, func() error {
		if profile == "" {
			var err error
			profile, err = pm.paths.activeProfileUnlocked()
			if err != nil {
				return err
			}
		}
		return pm.applyUnlocked(profile)
	})
}

// ApplyActive applies and returns the active profile under one lock, avoiding a
// marker/overlay race in environment computation.
func (pm *ProfileManager) ApplyActive() (string, error) {
	var profile string
	err := withConfigLock(pm.paths, func() error {
		var err error
		profile, err = pm.paths.activeProfileUnlocked()
		if err != nil {
			return err
		}
		return pm.applyUnlocked(profile)
	})
	return profile, err
}

func (pm *ProfileManager) applyUnlocked(profile string) error {
	srcRoot := filepath.Join(pm.paths.UserProfilesDir(), profile)
	if !util.DirExists(srcRoot) {
		return fmt.Errorf("profile '%s' not found in %s (run: local-data init)", profile, pm.paths.UserProfilesDir())
	}
	util.Log("Applying runtime config overlay for profile '%s'", profile)
	util.Log("  to: %s", pm.paths.CurrentConfDir())
	transactionID := newTransactionID()
	entry, err := pm.stageOverlay(srcRoot, profile, transactionID)
	if err != nil {
		return err
	}
	return publishStaged(pm.paths, transactionID, []stagedReplacement{entry})
}

func (pm *ProfileManager) stageOverlay(srcRoot, profile, transactionID string) (stagedReplacement, error) {
	if !util.DirExists(srcRoot) {
		return stagedReplacement{}, fmt.Errorf("profile '%s' not found at %s", profile, srcRoot)
	}
	entry := newStagedReplacement(pm.paths.CurrentConfDir(), "current-overlay", transactionID)
	if err := util.MkdirAll(filepath.Dir(entry.Target)); err != nil {
		return stagedReplacement{}, err
	}
	if err := util.CopyDir(srcRoot, entry.Stage); err != nil {
		_ = os.RemoveAll(entry.Stage)
		return stagedReplacement{}, fmt.Errorf("failed to stage profile configs: %w", err)
	}
	if err := pm.paths.runConfigHook("overlay.copy"); err != nil {
		_ = os.RemoveAll(entry.Stage)
		return stagedReplacement{}, err
	}

	hiveConfig := filepath.Join(entry.Stage, "hive", "hive-site.xml")
	if util.FileExists(hiveConfig) {
		sparkHiveConfig := filepath.Join(entry.Stage, "spark", "hive-site.xml")
		if err := util.CopyFile(hiveConfig, sparkHiveConfig); err != nil {
			_ = os.RemoveAll(entry.Stage)
			return stagedReplacement{}, fmt.Errorf("failed to stage Hive config for Spark: %w", err)
		}
		if err := pm.paths.runConfigHook("overlay.spark-copy"); err != nil {
			_ = os.RemoveAll(entry.Stage)
			return stagedReplacement{}, err
		}
	}
	if err := util.WriteFile(filepath.Join(entry.Stage, ".profile"), []byte(profile), util.PublicFileMode); err != nil {
		_ = os.RemoveAll(entry.Stage)
		return stagedReplacement{}, fmt.Errorf("failed to stage profile marker: %w", err)
	}
	if err := pm.paths.runConfigHook("overlay.marker-write"); err != nil {
		_ = os.RemoveAll(entry.Stage)
		return stagedReplacement{}, err
	}
	if err := checkOverlayPath(entry.Stage); err != nil {
		_ = os.RemoveAll(entry.Stage)
		return stagedReplacement{}, err
	}
	return entry, nil
}

func applySettingsUnder(root string, settings *Settings) error {
	dbType, err := metastore.NormalizeDBType(settings.DBType)
	if err != nil {
		return err
	}
	matches, err := filepath.Glob(filepath.Join(root, "*", "hive", "hive-site.xml"))
	if err != nil {
		return err
	}
	for _, path := range matches {
		cfg, err := util.ParseHadoopXML(path)
		if err != nil {
			return err
		}
		cfg.SetProperty("javax.jdo.option.ConnectionDriverName", metastore.DriverClass(dbType))
		cfg.SetProperty("javax.jdo.option.ConnectionURL", settings.DBURL)
		cfg.SetProperty("javax.jdo.option.ConnectionUserName", metastore.ConnectionUser(dbType, settings.User))
		cfg.SetProperty("javax.jdo.option.ConnectionPassword", settings.DBPassword)
		if err := cfg.WriteXML(path); err != nil {
			return err
		}
	}
	return nil
}

func checkOverlayPath(cur string) error {
	if !util.DirExists(cur) {
		return fmt.Errorf("runtime conf overlay not found: %s", cur)
	}
	hadoopConf := filepath.Join(cur, "hadoop")
	if util.DirExists(hadoopConf) {
		for _, name := range []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"} {
			if !util.FileExists(filepath.Join(hadoopConf, name)) {
				return fmt.Errorf("missing runtime Hadoop config: %s", filepath.Join(hadoopConf, name))
			}
		}
	}
	if !util.FileExists(filepath.Join(cur, "hive", "hive-site.xml")) {
		return fmt.Errorf("missing runtime Hive config: %s", filepath.Join(cur, "hive", "hive-site.xml"))
	}
	return nil
}

// Check verifies that the runtime config overlay exists and is valid
func (pm *ProfileManager) Check() error {
	return withConfigLock(pm.paths, func() error {
		return pm.checkUnlocked()
	})
}

func (pm *ProfileManager) checkUnlocked() error {
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
