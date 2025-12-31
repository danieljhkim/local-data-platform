package hive

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	// DefaultPostgresJDBCVersion is the default Postgres JDBC driver version
	DefaultPostgresJDBCVersion = "42.7.4"
)

// EnsurePostgresJDBCDriver ensures the Postgres JDBC driver is available
// Returns the path to the JAR file and any error
func EnsurePostgresJDBCDriver(hiveHome, baseDir string) (string, error) {
	version := DefaultPostgresJDBCVersion

	if hiveHome == "" {
		return "", fmt.Errorf("HIVE_HOME is not set; cannot install Postgres JDBC driver")
	}

	// Try to install in HIVE_HOME/lib first
	destDir := filepath.Join(hiveHome, "lib")
	jarPath := filepath.Join(destDir, fmt.Sprintf("postgresql-%s.jar", version))

	// Check if already exists
	if _, err := os.Stat(jarPath); err == nil {
		return jarPath, nil
	}

	// Check if directory is writable
	writable := isDirWritable(destDir)

	// If not writable, use fallback location
	if !writable {
		if baseDir == "" {
			homeDir, _ := os.UserHomeDir()
			baseDir = filepath.Join(homeDir, "local-data-platform")
		}

		destDir = filepath.Join(baseDir, "lib", "jars")
		jarPath = filepath.Join(destDir, fmt.Sprintf("postgresql-%s.jar", version))

		// Check if already exists in fallback location
		if _, err := os.Stat(jarPath); err == nil {
			util.Log("Using Postgres JDBC driver from HIVE_AUX_JARS_PATH: %s", jarPath)
			return jarPath, nil
		}

		util.Log("Hive lib not writable; will use HIVE_AUX_JARS_PATH=%s", jarPath)
	}

	// Create destination directory
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", destDir, err)
	}

	// Download the JAR
	util.Log("Downloading Postgres JDBC driver v%s...", version)

	url := fmt.Sprintf("https://repo1.maven.org/maven2/org/postgresql/postgresql/%s/postgresql-%s.jar", version, version)
	tmpPath := jarPath + ".tmp"

	if err := downloadFile(url, tmpPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to download JDBC jar from %s: %w", url, err)
	}

	// Move to final location
	if err := os.Rename(tmpPath, jarPath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to move JAR to %s: %w", jarPath, err)
	}

	util.Log("Successfully downloaded Postgres JDBC driver to %s", jarPath)
	return jarPath, nil
}

// isDirWritable checks if a directory is writable
func isDirWritable(dir string) bool {
	// Check if directory exists
	info, err := os.Stat(dir)
	if err != nil {
		return false
	}

	if !info.IsDir() {
		return false
	}

	// Try to create a temporary file
	tmpFile := filepath.Join(dir, ".write_test")
	f, err := os.Create(tmpFile)
	if err != nil {
		return false
	}
	f.Close()
	os.Remove(tmpFile)

	return true
}

// downloadFile downloads a file from a URL to a local path
func downloadFile(url, destPath string) error {
	// Create the file
	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check server response
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
