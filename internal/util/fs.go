package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// MkdirAll creates all specified directories with 0755 permissions
// Mirrors Bash mkdir -p
func MkdirAll(paths ...string) error {
	for _, path := range paths {
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", path, err)
		}
	}
	return nil
}

// WriteFile writes a file and then explicitly applies permissions, including for existing files.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	if perm == 0 {
		perm = PublicFileMode
	}
	if err := os.WriteFile(path, data, perm); err != nil {
		return err
	}
	if err := os.Chmod(path, perm); err != nil {
		return err
	}
	return nil
}

// CopyFile copies a file from src to dst
// Creates parent directories if needed
func CopyFile(src, dst string) error {
	// Ensure destination directory exists
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}

	mode := fileModeForContent(data, info.Mode().Perm())
	if err := WriteFile(dst, data, mode); err != nil {
		return fmt.Errorf("failed to write destination file: %w", err)
	}

	return nil
}

// FileExists checks if a file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DirExists checks if a directory exists
func DirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// IsDirEmpty checks if a directory is empty
func IsDirEmpty(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil // Non-existent directory is considered empty
		}
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// CopyDir recursively copies a directory from src to dst
func CopyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate destination path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			// Create directory
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		return CopyFile(path, dstPath)
	})
}
