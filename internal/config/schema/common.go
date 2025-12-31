package schema

import (
	"os"
	"os/user"
	"strings"
)

// Property represents a single configuration property
type Property struct {
	Name  string
	Value string
}

// TemplateContext holds variables for value substitution
type TemplateContext struct {
	User    string // {{USER}} - current username
	Home    string // {{HOME}} - user home directory
	BaseDir string // {{BASE_DIR}} - runtime base directory
}

// NewTemplateContext creates a new template context with current user info
func NewTemplateContext(baseDir string) (*TemplateContext, error) {
	return NewTemplateContextWithUser(baseDir, "")
}

// NewTemplateContextWithUser creates a new template context with optional user override
func NewTemplateContextWithUser(baseDir, userName string) (*TemplateContext, error) {
	ctx := &TemplateContext{
		BaseDir: baseDir,
	}

	// Get current user info
	if u, err := user.Current(); err == nil {
		ctx.User = u.Username
		ctx.Home = u.HomeDir
	} else {
		// Fallback to environment variables
		ctx.User = os.Getenv("USER")
		ctx.Home = os.Getenv("HOME")
	}

	// Override username if provided
	if userName != "" {
		ctx.User = userName
	}

	return ctx, nil
}

// Substitute replaces template variables in a string
func (ctx *TemplateContext) Substitute(value string) string {
	result := value
	result = strings.ReplaceAll(result, "{{USER}}", ctx.User)
	result = strings.ReplaceAll(result, "{{HOME}}", ctx.Home)
	result = strings.ReplaceAll(result, "{{BASE_DIR}}", ctx.BaseDir)
	return result
}

// ConfigSet represents all configuration for a profile
type ConfigSet struct {
	Hadoop *HadoopConfig
	Hive   *HiveConfig
	Spark  *SparkConfig
}

// Clone creates a deep copy of the ConfigSet
func (cs *ConfigSet) Clone() *ConfigSet {
	if cs == nil {
		return nil
	}

	clone := &ConfigSet{}

	if cs.Hadoop != nil {
		clone.Hadoop = cs.Hadoop.Clone()
	}
	if cs.Hive != nil {
		clone.Hive = cs.Hive.Clone()
	}
	if cs.Spark != nil {
		clone.Spark = cs.Spark.Clone()
	}

	return clone
}
