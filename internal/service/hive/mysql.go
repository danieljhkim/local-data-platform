package hive

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

const DefaultMySQLJDBCVersion = "8.4.0"

// EnsureMySQLJDBCDriver ensures a MySQL JDBC driver is available.
func EnsureMySQLJDBCDriver(hiveHome, sparkHome, baseDir string) (string, error) {
	version := DefaultMySQLJDBCVersion

	if hiveHome == "" {
		return "", fmt.Errorf("HIVE_HOME is not set; cannot locate MySQL JDBC driver")
	}

	var foundJar string
	primaryDir := filepath.Join(hiveHome, "lib")
	if jar, err := findMySQLJar(primaryDir); err == nil {
		foundJar = jar
	}

	var sparkJarsDir string
	if sparkHome != "" {
		sparkJarsDir = filepath.Join(sparkHome, "jars")
		if foundJar == "" {
			if jar, err := findMySQLJar(sparkJarsDir); err == nil {
				foundJar = jar
			}
		}
	}

	var fallbackDir string
	if strings.TrimSpace(baseDir) != "" {
		fallbackDir = filepath.Join(baseDir, "lib", "jars")
		if foundJar == "" {
			if jar, err := findMySQLJar(fallbackDir); err == nil {
				foundJar = jar
			}
		}
	}

	if foundJar == "" {
		downloadURL := fmt.Sprintf(
			"https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/%s/mysql-connector-j-%s.jar",
			version, version,
		)
		return "", fmt.Errorf(
			"MySQL JDBC driver not found (expected mysql-connector-j-*.jar or mysql-connector-java-*.jar in %s%s%s). Download: %s",
			primaryDir,
			optionalDir(sparkJarsDir),
			optionalDir(fallbackDir),
			downloadURL,
		)
	}

	if sparkJarsDir != "" {
		if _, err := findMySQLJar(sparkJarsDir); err != nil {
			if copyErr := ensureJarInSparkDir(foundJar, sparkJarsDir); copyErr != nil {
				util.Warn("Could not copy MySQL JDBC driver to %s: %v", sparkJarsDir, copyErr)
			}
		}
	}

	return foundJar, nil
}

func findMySQLJar(dir string) (string, error) {
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
		if strings.HasPrefix(name, "mysql-connector-j-") && strings.HasSuffix(name, ".jar") {
			candidates = append(candidates, filepath.Join(dir, name))
			continue
		}
		if strings.HasPrefix(name, "mysql-connector-java-") && strings.HasSuffix(name, ".jar") {
			candidates = append(candidates, filepath.Join(dir, name))
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no mysql jdbc jar found in %s", dir)
	}

	sort.Strings(candidates)
	return candidates[len(candidates)-1], nil
}

func optionalDir(dir string) string {
	if strings.TrimSpace(dir) == "" {
		return ""
	}
	return ", " + dir
}
