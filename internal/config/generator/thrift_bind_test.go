package generator

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/profiles"
)

// Hive 4.2.1 defaults hive.server2.thrift.bind.host (HiveConf) and
// hive.metastore.thrift.bind.host (MetastoreConf.THRIFT_BIND_HOST's Hive
// alias) to empty. An empty host makes the Thrift server socket a wildcard
// bind. These tests read generated hive-site.xml only. They do not start
// Hive and do not observe a listening socket.
//
// 127.0.0.1 matches the address a Hive 4.2.1 JVM uses for the existing
// client endpoint thrift://localhost:9083 when java.net.preferIPv6Addresses
// is left at its default: InetAddress.getByName("localhost") is 127.0.0.1.

const thriftLoopbackBindHost = "127.0.0.1"

func TestGeneratedProfiles_ThriftServicesBindToLoopback(t *testing.T) {
	reg := profiles.NewRegistry()
	gen := NewConfigGenerator()

	required := map[string]bool{"hdfs": false, "local": false}
	for _, profileName := range reg.List() {
		if _, ok := required[profileName]; ok {
			required[profileName] = true
		}
		t.Run(profileName, func(t *testing.T) {
			baseDir := t.TempDir()
			destDir := t.TempDir()
			if err := gen.Generate(profileName, baseDir, destDir); err != nil {
				t.Fatalf("Generate(%s) failed: %v", profileName, err)
			}

			hiveSite := filepath.Join(destDir, "hive", "hive-site.xml")
			assertSingleProperty(t, hiveSite, "hive.server2.thrift.bind.host", thriftLoopbackBindHost)
			assertSingleProperty(t, hiveSite, "hive.metastore.thrift.bind.host", thriftLoopbackBindHost)
			assertSingleProperty(t, hiveSite, "hive.metastore.uris", "thrift://localhost:9083")
			assertSingleProperty(t, hiveSite, "hive.server2.webui.host", thriftLoopbackBindHost)
			assertSingleProperty(t, hiveSite, "hive.server2.authentication", "NONE")
			assertSingleProperty(t, hiveSite, "hive.execution.engine", "mr")
		})
	}

	for name, seen := range required {
		if !seen {
			t.Errorf("built-in profile %s was not generated", name)
		}
	}
}

func TestGeneratedProfiles_ThriftBindHostOverrideReplacesLoopback(t *testing.T) {
	baseDir := writeOverrides(t, `profiles:
  hdfs:
    hive:
      hive.server2.thrift.bind.host: 192.0.2.10
      hive.metastore.thrift.bind.host: 192.0.2.11
  local:
    hive:
      hive.server2.thrift.bind.host: 192.0.2.12
      hive.metastore.thrift.bind.host: 192.0.2.13
`)
	gen := NewConfigGenerator()
	wants := map[string][2]string{
		"hdfs":  {"192.0.2.10", "192.0.2.11"},
		"local": {"192.0.2.12", "192.0.2.13"},
	}

	for profileName, bindHosts := range wants {
		t.Run(profileName, func(t *testing.T) {
			destDir := t.TempDir()
			if err := gen.Generate(profileName, baseDir, destDir); err != nil {
				t.Fatalf("Generate(%s) failed: %v", profileName, err)
			}

			hiveSite := filepath.Join(destDir, "hive", "hive-site.xml")
			assertSingleProperty(t, hiveSite, "hive.server2.thrift.bind.host", bindHosts[0])
			assertSingleProperty(t, hiveSite, "hive.metastore.thrift.bind.host", bindHosts[1])
			assertSingleProperty(t, hiveSite, "hive.metastore.uris", "thrift://localhost:9083")
			assertSingleProperty(t, hiveSite, "hive.server2.webui.host", thriftLoopbackBindHost)
		})
	}
}

func assertSingleProperty(t *testing.T, path, name, want string) {
	t.Helper()

	values := hadoopPropertyValues(t, path, name)
	if len(values) != 1 || values[0] != want {
		t.Errorf("%s: property %q = %q, want exactly [%q]", filepath.Base(path), name, values, want)
	}
}

func hadoopPropertyValues(t *testing.T, path, name string) []string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}

	var cfg hadoopXMLConfig
	if err := xml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("failed to parse %s: %v", path, err)
	}

	var values []string
	for _, prop := range cfg.Properties {
		if prop.Name == name {
			values = append(values, prop.Value)
		}
	}
	return values
}
