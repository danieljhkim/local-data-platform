package config

import (
	"fmt"
	"path/filepath"

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
	switch key {
	case "db-url":
		return a.updateHiveProperty("javax.jdo.option.ConnectionURL", newValue)
	case "db-password":
		return a.updateHiveProperty("javax.jdo.option.ConnectionPassword", newValue)
	case "user":
		return a.updateHiveProperty("javax.jdo.option.ConnectionUserName", newValue)
	case "base-dir":
		// Base dir is forward-only and applies on future generation.
		return nil
	default:
		return fmt.Errorf("unknown setting key %q", key)
	}
}

func (a *SettingsApplier) updateHiveProperty(property, value string) error {
	for _, path := range a.hiveSiteTargets() {
		if !util.FileExists(path) {
			continue
		}

		cfg, err := util.ParseHadoopXML(path)
		if err != nil {
			return fmt.Errorf("failed parsing %s: %w", path, err)
		}
		cfg.SetProperty(property, value)
		if err := cfg.WriteXML(path); err != nil {
			return fmt.Errorf("failed writing %s: %w", path, err)
		}
	}
	return nil
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
