package config

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// ProfileManager handles profile initialization, listing, setting, and overlay application
// Mirrors the functionality from lib/local_data/overlay.sh
type ProfileManager struct {
	paths *Paths
}

// NewProfileManager creates a new profile manager
func NewProfileManager(paths *Paths) *ProfileManager {
	return &ProfileManager{
		paths: paths,
	}
}

// Init initializes editable profiles from repo defaults
// Mirrors ld_profile_init
func (pm *ProfileManager) Init(force bool) error {
	src := pm.paths.RepoProfilesDir()
	dst := pm.paths.UserProfilesDir()

	// Check if source exists
	if !util.DirExists(src) {
		return fmt.Errorf("missing repo profiles directory: %s", src)
	}

	// Check if destination already exists
	if util.DirExists(dst) {
		if force {
			util.Log("Re-initializing profiles (overwriting): %s", dst)
			if err := os.RemoveAll(dst); err != nil {
				return fmt.Errorf("failed to remove existing profiles: %w", err)
			}
		} else {
			util.Log("Profiles already initialized: %s", dst)
			util.Log("  (use: local-data profile init --force to overwrite from repo defaults)")
			return nil
		}
	}

	util.Log("Initializing editable profiles under: %s", dst)

	// Ensure parent directory exists
	if err := util.MkdirAll(filepath.Dir(dst)); err != nil {
		return err
	}

	// Copy profiles directory
	return util.CopyDir(src, dst)
}

// List returns a sorted list of available profile names
// Mirrors ld_profile_list
func (pm *ProfileManager) List() ([]string, error) {
	pdir := pm.paths.ProfilesDir()

	if !util.DirExists(pdir) {
		return nil, fmt.Errorf("missing profiles directory: %s", pdir)
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
// Mirrors ld_profile_set
func (pm *ProfileManager) Set(profile string, fromRepo bool) error {
	if profile == "" {
		return fmt.Errorf("profile name required")
	}

	// Determine profile source directory
	var pdir string
	if fromRepo {
		pdir = pm.paths.RepoProfilesDir()
	} else {
		pdir = pm.paths.ProfilesDir()
	}

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
	util.Log("Using profiles from: %s", pdir)

	// Apply the overlay
	return pm.Apply(profile, fromRepo)
}

// Apply applies the runtime config overlay for a profile
// Mirrors ld_conf_apply
func (pm *ProfileManager) Apply(profile string, fromRepo bool) error {
	// If profile is empty, use active profile
	if profile == "" {
		var err error
		profile, err = pm.paths.ActiveProfile()
		if err != nil {
			return err
		}
	}

	// Determine profile source directory
	var pdir string
	if fromRepo {
		pdir = pm.paths.RepoProfilesDir()
	} else {
		pdir = pm.paths.ProfilesDir()
	}

	srcRoot := filepath.Join(pdir, profile)
	if !util.DirExists(srcRoot) {
		return fmt.Errorf("profile not found: %s", srcRoot)
	}

	dstRoot := pm.paths.CurrentConfDir()

	util.Log("Applying runtime config overlay for profile '%s'", profile)
	util.Log("  from: %s", srcRoot)
	util.Log("  to:   %s", dstRoot)

	// Get template variables
	vars, err := NewTemplateVars(pm.paths.BaseDir)
	if err != nil {
		return err
	}

	// Materialize as plain files (no symlinks)
	// Create base directories
	if err := util.MkdirAll(
		filepath.Join(dstRoot, "hive"),
		filepath.Join(dstRoot, "spark"),
	); err != nil {
		return err
	}

	// Hadoop XML (optional - some profiles like 'local' don't use Hadoop)
	hadoopSrc := filepath.Join(srcRoot, "hadoop")
	hadoopDst := filepath.Join(dstRoot, "hadoop")
	if util.DirExists(hadoopSrc) {
		if err := util.MkdirAll(hadoopDst); err != nil {
			return err
		}

		// Required Hadoop configs
		requiredConfigs := []string{
			"core-site.xml",
			"hdfs-site.xml",
			"mapred-site.xml",
			"yarn-site.xml",
		}

		for _, f := range requiredConfigs {
			dstPath := filepath.Join(hadoopDst, f)
			if err := CopyOrRenderFile(hadoopSrc, dstPath, f, vars); err != nil {
				return err
			}
		}

		// Optional scheduler configs
		optionalConfigs := []string{
			"capacity-scheduler.xml",
			"fair-scheduler.xml",
		}

		for _, f := range optionalConfigs {
			tmplPath := filepath.Join(hadoopSrc, f+".tmpl")
			plainPath := filepath.Join(hadoopSrc, f)
			if util.FileExists(tmplPath) || util.FileExists(plainPath) {
				dstPath := filepath.Join(hadoopDst, f)
				if err := CopyOrRenderFile(hadoopSrc, dstPath, f, vars); err != nil {
					// Don't fail on optional configs
					util.Warn("Failed to copy optional config %s: %v", f, err)
				}
			}
		}
	} else {
		// Profile doesn't use Hadoop - remove stale hadoop conf from previous profile
		if util.DirExists(hadoopDst) {
			if err := os.RemoveAll(hadoopDst); err != nil {
				util.Warn("Failed to remove stale hadoop conf: %v", err)
			}
		}
	}

	// Hive XML (required)
	hiveSrc := filepath.Join(srcRoot, "hive")
	hiveDst := filepath.Join(dstRoot, "hive")
	hiveConfig := filepath.Join(hiveDst, "hive-site.xml")
	if err := CopyOrRenderFile(hiveSrc, hiveConfig, "hive-site.xml", vars); err != nil {
		return fmt.Errorf("failed to copy required Hive config: %w", err)
	}

	// Spark defaults (optional but strongly expected)
	sparkSrc := filepath.Join(srcRoot, "spark")
	sparkDst := filepath.Join(dstRoot, "spark")
	sparkTmpl := filepath.Join(sparkSrc, "spark-defaults.conf.tmpl")
	sparkPlain := filepath.Join(sparkSrc, "spark-defaults.conf")
	if util.FileExists(sparkTmpl) || util.FileExists(sparkPlain) {
		sparkConfig := filepath.Join(sparkDst, "spark-defaults.conf")
		if err := CopyOrRenderFile(sparkSrc, sparkConfig, "spark-defaults.conf", vars); err != nil {
			util.Warn("Failed to copy Spark config: %v", err)
		}
	}

	// Copy hive-site.xml into Spark conf so PySpark/spark-submit find the metastore
	if util.FileExists(hiveConfig) {
		sparkHiveConfig := filepath.Join(sparkDst, "hive-site.xml")
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
// Mirrors ld_conf_check
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
