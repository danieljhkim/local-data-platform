package generator

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/profiles"
)

// reviewedWebEndpoints maps a generated config file to the property names
// whose value's bind host must not resolve to every interface (0.0.0.0) or
// be left empty. Each value may be a bare host (e.g. "127.0.0.1") or a
// "host:port" pair.
var reviewedWebEndpoints = map[string][]string{
	"hadoop/hdfs-site.xml": {
		"dfs.namenode.http-address",
		"dfs.datanode.http.address",
	},
	"hive/hive-site.xml": {
		"hive.server2.webui.host",
	},
}

func loadHadoopProperties(t *testing.T, path string) map[string]string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}

	var cfg hadoopXMLConfig
	if err := xml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("failed to parse %s: %v", path, err)
	}

	props := make(map[string]string, len(cfg.Properties))
	for _, p := range cfg.Properties {
		props[p.Name] = p.Value
	}
	return props
}

// bindHost extracts the host portion from a bare host or "host:port" value.
func bindHost(value string) string {
	if idx := strings.LastIndex(value, ":"); idx != -1 {
		return value[:idx]
	}
	return value
}

func TestGeneratedProfiles_AdministrativeWebEndpointsBindToLoopback(t *testing.T) {
	reg := profiles.NewRegistry()
	gen := NewConfigGenerator()

	for _, profileName := range reg.List() {
		t.Run(profileName, func(t *testing.T) {
			baseDir := t.TempDir()
			destDir := t.TempDir()

			if err := gen.Generate(profileName, baseDir, destDir); err != nil {
				t.Fatalf("Generate(%s) failed: %v", profileName, err)
			}

			for relPath, propertyNames := range reviewedWebEndpoints {
				fullPath := filepath.Join(destDir, relPath)
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					// Profile does not generate this config file (e.g. local
					// profile has no Hadoop config); nothing to check.
					continue
				}

				props := loadHadoopProperties(t, fullPath)
				for _, name := range propertyNames {
					value, ok := props[name]
					if !ok {
						t.Errorf("%s: expected property %q to be present", relPath, name)
						continue
					}

					host := bindHost(value)
					if host == "" {
						t.Errorf("%s: property %q has an empty bind host (value=%q)", relPath, name, value)
					}
					if host == "0.0.0.0" {
						t.Errorf("%s: property %q binds to 0.0.0.0 (value=%q), must be loopback-only", relPath, name, value)
					}
				}
			}
		})
	}
}
