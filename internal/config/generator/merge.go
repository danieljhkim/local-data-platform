package generator

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/config/schema"
	"gopkg.in/yaml.v3"
)

// OverrideConfig represents user overrides from YAML
type OverrideConfig struct {
	Profiles map[string]*ProfileOverride `yaml:"profiles"`
}

// ProfileOverride represents overrides for a single profile
type ProfileOverride struct {
	Hadoop *HadoopOverride        `yaml:"hadoop"`
	Hive   map[string]interface{} `yaml:"hive"`
	Spark  map[string]interface{} `yaml:"spark"`
}

// HadoopOverride represents overrides for Hadoop configs
type HadoopOverride struct {
	CoreSite          map[string]interface{} `yaml:"core-site"`
	HDFSSite          map[string]interface{} `yaml:"hdfs-site"`
	YarnSite          map[string]interface{} `yaml:"yarn-site"`
	MapredSite        map[string]interface{} `yaml:"mapred-site"`
	CapacityScheduler map[string]interface{} `yaml:"capacity-scheduler"`
}

// LoadOverrides loads user overrides from the override file
func LoadOverrides(baseDir string) (*OverrideConfig, error) {
	overridePath := filepath.Join(baseDir, "conf", "overrides.yaml")

	// Check if file exists
	data, err := os.ReadFile(overridePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No overrides file - return empty config
			return &OverrideConfig{Profiles: make(map[string]*ProfileOverride)}, nil
		}
		return nil, fmt.Errorf("failed to read overrides file: %w", err)
	}

	var cfg OverrideConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse overrides.yaml: %w", err)
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]*ProfileOverride)
	}

	return &cfg, nil
}

// MergeOverrides applies user overrides to a ConfigSet
func MergeOverrides(configSet *schema.ConfigSet, overrides *ProfileOverride) *schema.ConfigSet {
	if overrides == nil {
		return configSet
	}

	result := configSet.Clone()

	// Apply Hadoop overrides
	if result.Hadoop != nil && overrides.Hadoop != nil {
		if result.Hadoop.CoreSite != nil && overrides.Hadoop.CoreSite != nil {
			result.Hadoop.CoreSite.Extra = mergeProperties(result.Hadoop.CoreSite.Extra, overrides.Hadoop.CoreSite)
		}
		if result.Hadoop.HDFSSite != nil && overrides.Hadoop.HDFSSite != nil {
			result.Hadoop.HDFSSite.Extra = mergeProperties(result.Hadoop.HDFSSite.Extra, overrides.Hadoop.HDFSSite)
		}
		if result.Hadoop.YarnSite != nil && overrides.Hadoop.YarnSite != nil {
			result.Hadoop.YarnSite.Extra = mergeProperties(result.Hadoop.YarnSite.Extra, overrides.Hadoop.YarnSite)
		}
		if result.Hadoop.MapredSite != nil && overrides.Hadoop.MapredSite != nil {
			result.Hadoop.MapredSite.Extra = mergeProperties(result.Hadoop.MapredSite.Extra, overrides.Hadoop.MapredSite)
		}
		if result.Hadoop.CapacityScheduler != nil && overrides.Hadoop.CapacityScheduler != nil {
			result.Hadoop.CapacityScheduler.Extra = mergeProperties(result.Hadoop.CapacityScheduler.Extra, overrides.Hadoop.CapacityScheduler)
		}
	}

	// Apply Hive overrides
	if result.Hive != nil && overrides.Hive != nil {
		result.Hive.Extra = mergeProperties(result.Hive.Extra, overrides.Hive)
	}

	// Apply Spark overrides
	if result.Spark != nil && overrides.Spark != nil {
		result.Spark.Extra = mergeProperties(result.Spark.Extra, overrides.Spark)
	}

	return result
}

// mergeProperties merges override map into existing properties
func mergeProperties(existing []schema.Property, overrides map[string]interface{}) []schema.Property {
	// Create map of existing properties for quick lookup
	propMap := make(map[string]int)
	for i, p := range existing {
		propMap[p.Name] = i
	}

	// Add or replace properties
	for name, value := range overrides {
		prop := schema.Property{
			Name:  name,
			Value: fmt.Sprint(value),
		}

		if idx, ok := propMap[name]; ok {
			// Replace existing
			existing[idx] = prop
		} else {
			// Add new
			existing = append(existing, prop)
		}
	}

	return existing
}
