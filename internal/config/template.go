package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// TemplateVars holds variables for template rendering
// Mirrors the Bash template substitution in ld_render_template
type TemplateVars struct {
	User    string // {{USER}} - current username
	Home    string // {{HOME}} - user home directory
	BaseDir string // {{BASE_DIR}} - runtime base directory
}

// NewTemplateVars creates template variables with current values
func NewTemplateVars(baseDir string) (*TemplateVars, error) {
	// Get current user
	currentUser, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("failed to get current user: %w", err)
	}

	username := currentUser.Username
	home := currentUser.HomeDir

	// Prefer environment variables if set
	if envUser := os.Getenv("USER"); envUser != "" {
		username = envUser
	}
	if envHome := os.Getenv("HOME"); envHome != "" {
		home = envHome
	}

	return &TemplateVars{
		User:    username,
		Home:    home,
		BaseDir: baseDir,
	}, nil
}

// RenderTemplate renders a template file, substituting {{USER}}, {{HOME}}, {{BASE_DIR}}
// Mirrors ld_render_template function
// srcPath: source template file path
// dstPath: destination file path
// vars: template variables
func RenderTemplate(srcPath, dstPath string, vars *TemplateVars) error {
	// Read source file
	content, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("failed to read template %s: %w", srcPath, err)
	}

	// Perform string replacements
	// Using simple string replacement for exact Bash parity
	rendered := string(content)
	rendered = strings.ReplaceAll(rendered, "{{USER}}", vars.User)
	rendered = strings.ReplaceAll(rendered, "{{HOME}}", vars.Home)
	rendered = strings.ReplaceAll(rendered, "{{BASE_DIR}}", vars.BaseDir)

	// Ensure destination directory exists
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Write to destination file
	if err := os.WriteFile(dstPath, []byte(rendered), 0644); err != nil {
		return fmt.Errorf("failed to write rendered template to %s: %w", dstPath, err)
	}

	return nil
}

// CopyOrRenderFile copies or renders a profile file
// Prefers .tmpl version if it exists, otherwise copies plain file
// Mirrors ld_copy_or_render_profile_file
func CopyOrRenderFile(srcDir, dstPath, filename string, vars *TemplateVars) error {
	srcTmpl := filepath.Join(srcDir, filename+".tmpl")
	srcPlain := filepath.Join(srcDir, filename)

	// Check if template exists
	if _, err := os.Stat(srcTmpl); err == nil {
		// Render template
		return RenderTemplate(srcTmpl, dstPath, vars)
	}

	// Check if plain file exists
	if _, err := os.Stat(srcPlain); err == nil {
		// Copy plain file
		return copyFileDirect(srcPlain, dstPath)
	}

	return fmt.Errorf("missing required config in profile: %s (or template: %s)", srcPlain, srcTmpl)
}

// copyFileDirect copies a file from src to dst
func copyFileDirect(src, dst string) error {
	// Ensure destination directory exists
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Read source
	content, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}

	// Write to destination
	if err := os.WriteFile(dst, content, 0644); err != nil {
		return fmt.Errorf("failed to write destination file: %w", err)
	}

	return nil
}
