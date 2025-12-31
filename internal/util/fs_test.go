package util

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileExists(t *testing.T) {
	// Create temp file for testing
	tmpDir := t.TempDir()
	existingFile := filepath.Join(tmpDir, "exists.txt")
	os.WriteFile(existingFile, []byte("test"), 0644)

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "file exists",
			path:     existingFile,
			expected: true,
		},
		{
			name:     "file doesn't exist",
			path:     filepath.Join(tmpDir, "notfound.txt"),
			expected: false,
		},
		{
			name:     "path is directory",
			path:     tmpDir,
			expected: true, // FileExists returns true for both files and directories
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FileExists(tt.path)
			if result != tt.expected {
				t.Errorf("FileExists(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name          string
		setupSrc      func() string
		dst           string
		expectError   bool
		validateCopy  func(t *testing.T, dst string)
	}{
		{
			name: "copy regular file",
			setupSrc: func() string {
				src := filepath.Join(tmpDir, "source.txt")
				os.WriteFile(src, []byte("hello world"), 0644)
				return src
			},
			dst:         filepath.Join(tmpDir, "dest1.txt"),
			expectError: false,
			validateCopy: func(t *testing.T, dst string) {
				content, err := os.ReadFile(dst)
				if err != nil {
					t.Fatalf("Failed to read copied file: %v", err)
				}
				if string(content) != "hello world" {
					t.Errorf("Content = %q, want %q", string(content), "hello world")
				}
			},
		},
		{
			name: "copy file (permissions not preserved)",
			setupSrc: func() string {
				src := filepath.Join(tmpDir, "source_perm.txt")
				os.WriteFile(src, []byte("test"), 0755)
				return src
			},
			dst:         filepath.Join(tmpDir, "dest2.txt"),
			expectError: false,
			validateCopy: func(t *testing.T, dst string) {
				// CopyFile does not preserve permissions, just verify file exists
				if !FileExists(dst) {
					t.Error("File not copied")
				}
			},
		},
		{
			name: "source file doesn't exist",
			setupSrc: func() string {
				return filepath.Join(tmpDir, "nonexistent.txt")
			},
			dst:         filepath.Join(tmpDir, "dest3.txt"),
			expectError: true,
			validateCopy: func(t *testing.T, dst string) {
				// No validation needed for error case
			},
		},
		{
			name: "destination already exists (overwrite)",
			setupSrc: func() string {
				src := filepath.Join(tmpDir, "source_overwrite.txt")
				os.WriteFile(src, []byte("new content"), 0644)
				dst := filepath.Join(tmpDir, "dest4.txt")
				os.WriteFile(dst, []byte("old content"), 0644)
				return src
			},
			dst:         filepath.Join(tmpDir, "dest4.txt"),
			expectError: false,
			validateCopy: func(t *testing.T, dst string) {
				content, _ := os.ReadFile(dst)
				if string(content) != "new content" {
					t.Errorf("Content = %q, want %q (should overwrite)", string(content), "new content")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := tt.setupSrc()
			err := CopyFile(src, tt.dst)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				tt.validateCopy(t, tt.dst)
			}
		})
	}
}

func TestIsDirEmpty(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		setup       func() string
		expected    bool
		expectError bool
	}{
		{
			name: "empty directory",
			setup: func() string {
				dir := filepath.Join(tmpDir, "empty")
				os.MkdirAll(dir, 0755)
				return dir
			},
			expected:    true,
			expectError: false,
		},
		{
			name: "directory with files",
			setup: func() string {
				dir := filepath.Join(tmpDir, "withfiles")
				os.MkdirAll(dir, 0755)
				os.WriteFile(filepath.Join(dir, "file.txt"), []byte("test"), 0644)
				return dir
			},
			expected:    false,
			expectError: false,
		},
		{
			name: "directory with hidden files",
			setup: func() string {
				dir := filepath.Join(tmpDir, "withhidden")
				os.MkdirAll(dir, 0755)
				os.WriteFile(filepath.Join(dir, ".hidden"), []byte("test"), 0644)
				return dir
			},
			expected:    false,
			expectError: false,
		},
		{
			name: "directory doesn't exist",
			setup: func() string {
				return filepath.Join(tmpDir, "nonexistent")
			},
			expected:    true, // Non-existent directory is considered empty
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.setup()
			result, err := IsDirEmpty(dir)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("IsDirEmpty(%q) = %v, want %v", dir, result, tt.expected)
				}
			}
		})
	}
}

func TestMkdirAll(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		paths       []string
		expectError bool
	}{
		{
			name:        "create single directory",
			paths:       []string{filepath.Join(tmpDir, "single")},
			expectError: false,
		},
		{
			name:        "create nested directories",
			paths:       []string{filepath.Join(tmpDir, "a", "b", "c")},
			expectError: false,
		},
		{
			name:        "create multiple directories",
			paths:       []string{filepath.Join(tmpDir, "d1"), filepath.Join(tmpDir, "d2")},
			expectError: false,
		},
		{
			name:        "directory already exists",
			paths:       []string{tmpDir},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := MkdirAll(tt.paths...)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				// Verify all directories exist
				for _, path := range tt.paths {
					if _, err := os.Stat(path); os.IsNotExist(err) {
						t.Errorf("Directory not created: %s", path)
					}
				}
			}
		})
	}
}
