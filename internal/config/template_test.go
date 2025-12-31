package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRenderTemplate(t *testing.T) {
	// Create temporary directory for test files
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		content  string
		vars     *TemplateVars
		expected string
	}{
		{
			name:    "replace USER",
			content: "User: {{USER}}",
			vars: &TemplateVars{
				User:    "testuser",
				Home:    "/home/testuser",
				BaseDir: "/data",
			},
			expected: "User: testuser",
		},
		{
			name:    "replace HOME",
			content: "Home: {{HOME}}",
			vars: &TemplateVars{
				User:    "testuser",
				Home:    "/home/testuser",
				BaseDir: "/data",
			},
			expected: "Home: /home/testuser",
		},
		{
			name:    "replace BASE_DIR",
			content: "BaseDir: {{BASE_DIR}}",
			vars: &TemplateVars{
				User:    "testuser",
				Home:    "/home/testuser",
				BaseDir: "/data",
			},
			expected: "BaseDir: /data",
		},
		{
			name:    "replace all",
			content: "{{USER}} lives in {{HOME}} with data in {{BASE_DIR}}",
			vars: &TemplateVars{
				User:    "alice",
				Home:    "/home/alice",
				BaseDir: "/var/data",
			},
			expected: "alice lives in /home/alice with data in /var/data",
		},
		{
			name:    "no replacements",
			content: "This is a plain file",
			vars: &TemplateVars{
				User:    "testuser",
				Home:    "/home/testuser",
				BaseDir: "/data",
			},
			expected: "This is a plain file",
		},
		{
			name:    "multiline",
			content: "User: {{USER}}\nHome: {{HOME}}\nBase: {{BASE_DIR}}",
			vars: &TemplateVars{
				User:    "bob",
				Home:    "/home/bob",
				BaseDir: "/opt/data",
			},
			expected: "User: bob\nHome: /home/bob\nBase: /opt/data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create source file
			srcPath := filepath.Join(tmpDir, "src_"+tt.name)
			if err := os.WriteFile(srcPath, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to create source file: %v", err)
			}

			// Render template
			dstPath := filepath.Join(tmpDir, "dst_"+tt.name)
			if err := RenderTemplate(srcPath, dstPath, tt.vars); err != nil {
				t.Fatalf("RenderTemplate() error = %v", err)
			}

			// Read result
			result, err := os.ReadFile(dstPath)
			if err != nil {
				t.Fatalf("Failed to read result file: %v", err)
			}

			if string(result) != tt.expected {
				t.Errorf("RenderTemplate() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func TestRenderOrCopyFile(t *testing.T) {
	tmpDir := t.TempDir()

	vars := &TemplateVars{
		User:    "testuser",
		Home:    "/home/testuser",
		BaseDir: "/data",
	}

	tests := []struct {
		name           string
		filename       string
		content        string
		shouldTemplate bool
		expected       string
	}{
		{
			name:           "XML template file",
			filename:       "config.xml",
			content:        "<user>{{USER}}</user>",
			shouldTemplate: true,
			expected:       "<user>testuser</user>",
		},
		{
			name:           "conf template file",
			filename:       "settings.conf",
			content:        "home={{HOME}}",
			shouldTemplate: true,
			expected:       "home=/home/testuser",
		},
		{
			name:           "properties template file",
			filename:       "app.properties",
			content:        "base.dir={{BASE_DIR}}",
			shouldTemplate: true,
			expected:       "base.dir=/data",
		},
		{
			name:           "plain file should be copied",
			filename:       "data.bin",
			content:        "binary data {{USER}}",
			shouldTemplate: false,
			expected:       "binary data {{USER}}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create source directory and file
			srcDir := filepath.Join(tmpDir, "src_"+tt.name)
			if err := os.MkdirAll(srcDir, 0755); err != nil {
				t.Fatalf("Failed to create source dir: %v", err)
			}

			// Create .tmpl file if should template, otherwise plain file
			var srcFile string
			if tt.shouldTemplate {
				srcFile = filepath.Join(srcDir, tt.filename+".tmpl")
			} else {
				srcFile = filepath.Join(srcDir, tt.filename)
			}

			if err := os.WriteFile(srcFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to create source file: %v", err)
			}

			// Render or copy
			dstPath := filepath.Join(tmpDir, "dst_"+tt.name, tt.filename)
			if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
				t.Fatalf("Failed to create dest dir: %v", err)
			}

			if err := CopyOrRenderFile(srcDir, dstPath, tt.filename, vars); err != nil {
				t.Fatalf("CopyOrRenderFile() error = %v", err)
			}

			// Read result
			result, err := os.ReadFile(dstPath)
			if err != nil {
				t.Fatalf("Failed to read result file: %v", err)
			}

			if string(result) != tt.expected {
				t.Errorf("CopyOrRenderFile() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}
