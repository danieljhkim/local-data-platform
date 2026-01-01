package hive

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	// DefaultPostgresJDBCVersion is used only to construct a helpful download URL (we do NOT enforce an exact version).
	DefaultPostgresJDBCVersion = "42.7.4"
)

// EnsurePostgresJDBCDriver ensures the Postgres JDBC driver is available
// Returns the path to the JAR file and any error
// Also ensures the JAR is present in SPARK_HOME/jars if sparkHome is provided
func EnsurePostgresJDBCDriver(hiveHome, sparkHome, baseDir string) (string, error) {
	// NOTE: we intentionally do NOT enforce a specific version. Any compatible
	// postgresql JDBC driver jar is fine.
	version := DefaultPostgresJDBCVersion

	if hiveHome == "" {
		return "", fmt.Errorf("HIVE_HOME is not set; cannot locate Postgres JDBC driver")
	}

	var foundJar string

	// Primary location: $HIVE_HOME/lib (Homebrew Hive often points here)
	primaryDir := filepath.Join(hiveHome, "lib")
	if jar, err := findPostgresJar(primaryDir); err == nil {
		foundJar = jar
	}

	// Check SPARK_HOME/jars as another source
	var sparkJarsDir string
	if sparkHome != "" {
		sparkJarsDir = filepath.Join(sparkHome, "jars")
		if foundJar == "" {
			if jar, err := findPostgresJar(sparkJarsDir); err == nil {
				foundJar = jar
			}
		}
	}

	// Fallback location: user-controlled directory (HIVE_AUX_JARS_PATH). Only use
	// this if the caller provided baseDir; do not guess a random path.
	var fallbackDir, fallbackJarExample string
	if strings.TrimSpace(baseDir) != "" {
		fallbackDir = filepath.Join(baseDir, "lib", "jars")
		if foundJar == "" {
			if jar, err := findPostgresJar(fallbackDir); err == nil {
				util.Log("Using Postgres JDBC driver from HIVE_AUX_JARS_PATH: %s", jar)
				foundJar = jar
			}
		}
		fallbackJarExample = filepath.Join(fallbackDir, "postgresql-<version>.jar")
	}

	// If we didn't find the JAR anywhere, return an error
	if foundJar == "" {
		downloadURL := fmt.Sprintf(
			"https://repo1.maven.org/maven2/org/postgresql/postgresql/%s/postgresql-%s.jar",
			version, version,
		)

		// Provide a very explicit error. Mention both the "any version is fine" rule
		// and the exact directories we scan.
		msg := "Postgres JDBC driver not found.\n\n" +
			"We looked for any file matching:\n" +
			"  postgresql-*.jar\n\n" +
			"In the following locations:\n" +
			"  1) " + primaryDir + "\n"
		if sparkJarsDir != "" {
			msg += "  2) " + sparkJarsDir + "\n"
		}
		if fallbackDir != "" {
			msg += "  3) " + fallbackDir + "\n"
		}
		msg += "\n" +
			"Please download the PostgreSQL JDBC driver (any recent version is fine):\n" +
			"  " + downloadURL + "\n\n" +
			"Then place the JAR in ONE of the locations above.\n"
		if fallbackDir != "" {
			msg += "\nIf using location (3), ensure HIVE_AUX_JARS_PATH includes the JAR path, e.g.:\n" +
				"  " + fallbackJarExample + "\n"
		} else {
			msg += "\nTip: if you want to use a custom jars directory, pass baseDir from the CLI/config and set HIVE_AUX_JARS_PATH accordingly.\n"
		}

		return "", fmt.Errorf("%s", msg)
	}

	// Ensure the JAR is also present in SPARK_HOME/jars
	if sparkJarsDir != "" {
		if _, err := findPostgresJar(sparkJarsDir); err != nil {
			// JAR not in Spark jars dir, try to copy it
			if err := ensureJarInSparkDir(foundJar, sparkJarsDir); err != nil {
				util.Warn("Could not copy Postgres JDBC driver to %s: %v", sparkJarsDir, err)
			}
		}
	}

	return foundJar, nil
}

// ensureJarInSparkDir copies a JAR to SPARK_HOME/jars if possible
func ensureJarInSparkDir(srcJar, sparkJarsDir string) error {
	// Check if spark jars dir exists
	if _, err := os.Stat(sparkJarsDir); os.IsNotExist(err) {
		return fmt.Errorf("spark jars directory does not exist: %s", sparkJarsDir)
	}

	jarName := filepath.Base(srcJar)
	dstJar := filepath.Join(sparkJarsDir, jarName)

	// Check if already exists
	if _, err := os.Stat(dstJar); err == nil {
		return nil // Already exists
	}

	// Try to copy
	if err := util.CopyFile(srcJar, dstJar); err != nil {
		return fmt.Errorf("failed to copy %s to %s: %w", srcJar, dstJar, err)
	}

	util.Log("Copied Postgres JDBC driver to %s", dstJar)
	return nil
}

// findPostgresJar returns the path to a PostgreSQL JDBC driver jar in dir.
// We accept any jar matching "postgresql-*.jar" and prefer the lexicographically
// greatest file name when multiple are present (usually the newest version).
func findPostgresJar(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var candidates []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "postgresql-") && strings.HasSuffix(name, ".jar") {
			candidates = append(candidates, filepath.Join(dir, name))
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no postgres jdbc jar found in %s", dir)
	}

	sort.Strings(candidates)
	return candidates[len(candidates)-1], nil
}
