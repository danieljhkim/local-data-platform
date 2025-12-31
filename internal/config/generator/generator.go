package generator

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/config/profiles"
	"github.com/danieljhkim/local-data-platform/internal/config/schema"
)

// InitOptions holds optional parameters for profile initialization
type InitOptions struct {
	User       string // Override username
	DBUrl      string // Override database connection URL
	DBPassword string // Override database password
}

// ConfigGenerator generates configuration files for profiles
type ConfigGenerator struct {
	registry *profiles.Registry
}

// NewConfigGenerator creates a new generator
func NewConfigGenerator() *ConfigGenerator {
	return &ConfigGenerator{
		registry: profiles.NewRegistry(),
	}
}

// HasProfile checks if a profile is a built-in profile
func (g *ConfigGenerator) HasProfile(name string) bool {
	return g.registry.Has(name)
}

// List returns all available built-in profile names
func (g *ConfigGenerator) List() []string {
	return g.registry.List()
}

// InitProfiles generates all built-in profiles to the profiles directory
// This creates $destProfilesDir/{hdfs,local}/ with config files
func (g *ConfigGenerator) InitProfiles(baseDir, destProfilesDir string, opts *InitOptions) error {
	for _, profileName := range g.registry.List() {
		profileDir := filepath.Join(destProfilesDir, profileName)
		if err := g.GenerateWithOptions(profileName, baseDir, profileDir, opts); err != nil {
			return fmt.Errorf("failed to generate profile '%s': %w", profileName, err)
		}
	}
	return nil
}

// GenerateWithOptions generates all config files for a profile with optional overrides
func (g *ConfigGenerator) GenerateWithOptions(profileName, baseDir, destDir string, opts *InitOptions) error {
	// 1. Get base profile from registry
	profile, err := g.registry.Get(profileName)
	if err != nil {
		return err
	}

	// 2. Load user overrides from YAML
	overrides, err := LoadOverrides(baseDir)
	if err != nil {
		return fmt.Errorf("failed to load overrides: %w", err)
	}

	// 3. Merge overrides into config
	configSet := profile.ConfigSet
	if profileOverride, ok := overrides.Profiles[profileName]; ok {
		configSet = MergeOverrides(configSet, profileOverride)
	}

	// 4. Apply CLI options (these take precedence over YAML overrides)
	if opts != nil {
		configSet = g.applyInitOptions(configSet, opts)
	}

	// 5. Create template context with optional user override
	userName := ""
	if opts != nil {
		userName = opts.User
	}
	ctx, err := schema.NewTemplateContextWithUser(baseDir, userName)
	if err != nil {
		return fmt.Errorf("failed to create template context: %w", err)
	}

	// 6. Generate files
	if configSet.Hadoop != nil {
		if err := g.generateHadoop(configSet.Hadoop, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Hadoop config: %w", err)
		}
	} else {
		// No Hadoop config - remove stale hadoop conf from previous profile
		hadoopDir := filepath.Join(destDir, "hadoop")
		if _, err := os.Stat(hadoopDir); err == nil {
			if err := os.RemoveAll(hadoopDir); err != nil {
				return fmt.Errorf("failed to remove stale hadoop conf: %w", err)
			}
		}
	}

	if configSet.Hive != nil {
		if err := g.generateHive(configSet.Hive, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Hive config: %w", err)
		}
	}

	if configSet.Spark != nil {
		if err := g.generateSpark(configSet.Spark, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Spark config: %w", err)
		}
	}

	return nil
}

// applyInitOptions applies CLI options to the config set
func (g *ConfigGenerator) applyInitOptions(configSet *schema.ConfigSet, opts *InitOptions) *schema.ConfigSet {
	if opts == nil {
		return configSet
	}

	// Clone to avoid modifying the original
	result := configSet.Clone()

	// Apply DB options to Hive config
	if result.Hive != nil {
		if opts.DBUrl != "" {
			result.Hive.ConnectionURL = opts.DBUrl
		}
		if opts.DBPassword != "" {
			result.Hive.ConnectionPassword = opts.DBPassword
		}
	}

	return result
}

// Generate generates all config files for a profile
func (g *ConfigGenerator) Generate(profileName, baseDir, destDir string) error {
	// 1. Get base profile from registry
	profile, err := g.registry.Get(profileName)
	if err != nil {
		return err
	}

	// 2. Load user overrides
	overrides, err := LoadOverrides(baseDir)
	if err != nil {
		return fmt.Errorf("failed to load overrides: %w", err)
	}

	// 3. Merge overrides into config
	configSet := profile.ConfigSet
	if profileOverride, ok := overrides.Profiles[profileName]; ok {
		configSet = MergeOverrides(configSet, profileOverride)
	}

	// 4. Create template context
	ctx, err := schema.NewTemplateContext(baseDir)
	if err != nil {
		return fmt.Errorf("failed to create template context: %w", err)
	}

	// 5. Generate files
	if configSet.Hadoop != nil {
		if err := g.generateHadoop(configSet.Hadoop, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Hadoop config: %w", err)
		}
	} else {
		// No Hadoop config - remove stale hadoop conf from previous profile
		hadoopDir := filepath.Join(destDir, "hadoop")
		if _, err := os.Stat(hadoopDir); err == nil {
			if err := os.RemoveAll(hadoopDir); err != nil {
				return fmt.Errorf("failed to remove stale hadoop conf: %w", err)
			}
		}
	}

	if configSet.Hive != nil {
		if err := g.generateHive(configSet.Hive, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Hive config: %w", err)
		}
	}

	if configSet.Spark != nil {
		if err := g.generateSpark(configSet.Spark, ctx, destDir); err != nil {
			return fmt.Errorf("failed to generate Spark config: %w", err)
		}
	}

	return nil
}

func (g *ConfigGenerator) generateHadoop(cfg *schema.HadoopConfig, ctx *schema.TemplateContext, destDir string) error {
	hadoopDir := filepath.Join(destDir, "hadoop")

	// Ensure directory exists
	if err := os.MkdirAll(hadoopDir, 0755); err != nil {
		return err
	}

	if cfg.CoreSite != nil {
		props := cfg.CoreSite.ToProperties(ctx)
		if err := WriteHadoopXML(props, filepath.Join(hadoopDir, "core-site.xml")); err != nil {
			return fmt.Errorf("core-site.xml: %w", err)
		}
	}

	if cfg.HDFSSite != nil {
		props := cfg.HDFSSite.ToProperties(ctx)
		if err := WriteHadoopXML(props, filepath.Join(hadoopDir, "hdfs-site.xml")); err != nil {
			return fmt.Errorf("hdfs-site.xml: %w", err)
		}
	}

	if cfg.YarnSite != nil {
		props := cfg.YarnSite.ToProperties(ctx)
		if err := WriteHadoopXML(props, filepath.Join(hadoopDir, "yarn-site.xml")); err != nil {
			return fmt.Errorf("yarn-site.xml: %w", err)
		}
	}

	if cfg.MapredSite != nil {
		props := cfg.MapredSite.ToProperties(ctx)
		if err := WriteHadoopXML(props, filepath.Join(hadoopDir, "mapred-site.xml")); err != nil {
			return fmt.Errorf("mapred-site.xml: %w", err)
		}
	}

	if cfg.CapacityScheduler != nil {
		props := cfg.CapacityScheduler.ToProperties(ctx)
		if err := WriteHadoopXML(props, filepath.Join(hadoopDir, "capacity-scheduler.xml")); err != nil {
			return fmt.Errorf("capacity-scheduler.xml: %w", err)
		}
	}

	return nil
}

func (g *ConfigGenerator) generateHive(cfg *schema.HiveConfig, ctx *schema.TemplateContext, destDir string) error {
	hiveDir := filepath.Join(destDir, "hive")

	// Ensure directory exists
	if err := os.MkdirAll(hiveDir, 0755); err != nil {
		return err
	}

	props := cfg.ToProperties(ctx)
	return WriteHadoopXML(props, filepath.Join(hiveDir, "hive-site.xml"))
}

func (g *ConfigGenerator) generateSpark(cfg *schema.SparkConfig, ctx *schema.TemplateContext, destDir string) error {
	sparkDir := filepath.Join(destDir, "spark")

	// Ensure directory exists
	if err := os.MkdirAll(sparkDir, 0755); err != nil {
		return err
	}

	props := cfg.ToProperties(ctx)
	return WriteSparkConf(props, filepath.Join(sparkDir, "spark-defaults.conf"))
}
