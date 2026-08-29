package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// SettingsApplier updates generated config files in response to setting changes.
type SettingsApplier struct {
	paths *Paths
}

// NewSettingsApplier creates a settings applier.
func NewSettingsApplier(paths *Paths) *SettingsApplier {
	return &SettingsApplier{paths: paths}
}

// Apply propagates a setting change to relevant generated config files.
func (a *SettingsApplier) Apply(key, oldValue, newValue string) error {
	_ = oldValue
	return withConfigLock(a.paths, func() error {
		if key == "base-dir" {
			return nil
		}
		settings, err := NewSettingsManager(a.paths).loadOrDefaultUnlocked()
		if err != nil {
			return err
		}
		var update func(*util.HadoopConfiguration) error
		switch key {
		case "db-type":
			dbType, err := metastore.NormalizeDBType(settings.DBType)
			if err != nil {
				return err
			}
			update = func(cfg *util.HadoopConfiguration) error {
				cfg.SetProperty("javax.jdo.option.ConnectionDriverName", metastore.DriverClass(dbType))
				cfg.SetProperty("javax.jdo.option.ConnectionURL", settings.DBURL)
				cfg.SetProperty("javax.jdo.option.ConnectionUserName", metastore.ConnectionUser(dbType, settings.User))
				return nil
			}
		case "db-url":
			update = func(cfg *util.HadoopConfiguration) error {
				cfg.SetProperty("javax.jdo.option.ConnectionURL", newValue)
				return nil
			}
		case "db-password":
			update = func(cfg *util.HadoopConfiguration) error {
				cfg.SetProperty("javax.jdo.option.ConnectionPassword", newValue)
				return nil
			}
		case "user":
			dbType, err := metastore.NormalizeDBType(settings.DBType)
			if err != nil {
				return err
			}
			update = func(cfg *util.HadoopConfiguration) error {
				cfg.SetProperty("javax.jdo.option.ConnectionUserName", metastore.ConnectionUser(dbType, newValue))
				return nil
			}
		default:
			return fmt.Errorf("unknown setting key %q", key)
		}
		transactionID := newTransactionID()
		entries, err := a.stageHiveUpdate(transactionID, update)
		if err != nil {
			return err
		}
		return publishStaged(a.paths, transactionID, entries)
	})
}

func (a *SettingsApplier) stageHiveSettings(settings *Settings, transactionID string) ([]stagedReplacement, error) {
	dbType, err := metastore.NormalizeDBType(settings.DBType)
	if err != nil {
		return nil, err
	}

	return a.stageHiveUpdate(transactionID, func(cfg *util.HadoopConfiguration) error {
		cfg.SetProperty("javax.jdo.option.ConnectionDriverName", metastore.DriverClass(dbType))
		cfg.SetProperty("javax.jdo.option.ConnectionURL", settings.DBURL)
		cfg.SetProperty("javax.jdo.option.ConnectionUserName", metastore.ConnectionUser(dbType, settings.User))
		cfg.SetProperty("javax.jdo.option.ConnectionPassword", settings.DBPassword)
		return nil
	})
}

func (a *SettingsApplier) stageHiveUpdate(transactionID string, update func(*util.HadoopConfiguration) error) ([]stagedReplacement, error) {
	var entries []stagedReplacement
	cleanup := func() {
		for _, staged := range entries {
			_ = os.RemoveAll(staged.Stage)
		}
	}
	for _, path := range a.hiveSiteTargets() {
		if !util.FileExists(path) {
			continue
		}

		cfg, err := util.ParseHadoopXML(path)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("failed parsing %s: %w", path, err)
		}
		if err := update(cfg); err != nil {
			cleanup()
			return nil, err
		}

		entry := newStagedReplacement(path, "hive-xml:"+relativeConfigPath(a.paths, path), transactionID)
		if err := a.paths.runConfigHook("xml.write:" + relativeConfigPath(a.paths, path)); err != nil {
			cleanup()
			return nil, err
		}
		if err := cfg.WriteXML(entry.Stage); err != nil {
			_ = os.RemoveAll(entry.Stage)
			cleanup()
			return nil, fmt.Errorf("failed writing staged %s: %w", path, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (a *SettingsApplier) hiveSiteTargets() []string {
	targets := []string{
		filepath.Join(a.paths.CurrentHiveConf(), "hive-site.xml"),
		filepath.Join(a.paths.CurrentSparkConf(), "hive-site.xml"),
	}

	matches, _ := filepath.Glob(filepath.Join(a.paths.UserProfilesDir(), "*", "hive", "hive-site.xml"))
	targets = append(targets, matches...)

	seen := make(map[string]struct{}, len(targets))
	dedup := make([]string, 0, len(targets))
	for _, path := range targets {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		dedup = append(dedup, path)
	}

	return dedup
}
