package util

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTestFile(t *testing.T, path string, data []byte, perm os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, perm); err != nil {
		t.Fatalf("failed to write test file %s: %v", path, err)
	}
}

func mkdirTestDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create test dir %s: %v", path, err)
	}
}

func TestFileExists(t *testing.T) {
	// Create temp file for testing
	tmpDir := t.TempDir()
	existingFile := filepath.Join(tmpDir, "exists.txt")
	writeTestFile(t, existingFile, []byte("test"), 0644)

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
		name         string
		setupSrc     func() string
		dst          string
		expectError  bool
		validateCopy func(t *testing.T, dst string)
	}{
		{
			name: "copy regular file",
			setupSrc: func() string {
				src := filepath.Join(tmpDir, "source.txt")
				writeTestFile(t, src, []byte("hello world"), 0644)
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
				writeTestFile(t, src, []byte("test"), 0755)
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
				writeTestFile(t, src, []byte("new content"), 0644)
				dst := filepath.Join(tmpDir, "dest4.txt")
				writeTestFile(t, dst, []byte("old content"), 0644)
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

func TestCopyFile_SensitiveHadoopXMLIsPrivate(t *testing.T) {
	tmpDir := t.TempDir()
	src := filepath.Join(tmpDir, "hive-site.xml")
	dst := filepath.Join(tmpDir, "spark", "hive-site.xml")
	content := `<configuration>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>secret</value></property>
</configuration>
`
	if err := os.WriteFile(src, []byte(content), 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	if err := CopyFile(src, dst); err != nil {
		t.Fatalf("CopyFile() error: %v", err)
	}

	info, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat destination: %v", err)
	}
	if got := info.Mode().Perm(); got&0077 != 0 {
		t.Fatalf("destination mode = %v, should not be group- or world-readable", got)
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
				mkdirTestDir(t, dir)
				return dir
			},
			expected:    true,
			expectError: false,
		},
		{
			name: "directory with files",
			setup: func() string {
				dir := filepath.Join(tmpDir, "withfiles")
				mkdirTestDir(t, dir)
				writeTestFile(t, filepath.Join(dir, "file.txt"), []byte("test"), 0644)
				return dir
			},
			expected:    false,
			expectError: false,
		},
		{
			name: "directory with hidden files",
			setup: func() string {
				dir := filepath.Join(tmpDir, "withhidden")
				mkdirTestDir(t, dir)
				writeTestFile(t, filepath.Join(dir, ".hidden"), []byte("test"), 0644)
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
