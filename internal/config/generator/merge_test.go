package generator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/schema"
)

func TestLoadOverridesPreservesMappingsAndAnchors(t *testing.T) {
	baseDir := writeOverrides(t, `profiles:
  hdfs:
    hadoop:
      core-site: &shared
        hadoop.security.authorization: true
        hadoop.tmp.dir: /tmp/hadoop
      hdfs-site: *shared
      yarn-site:
        yarn.nodemanager.resource.memory-mb: 4096
      mapred-site:
        mapreduce.framework.name: yarn
      capacity-scheduler:
        yarn.scheduler.capacity.root.queues: default
    hive:
      javax.jdo.option.ConnectionURL: jdbc:postgresql://localhost/hive
    spark:
      spark.sql.shuffle.partitions: 200
`)

	overrides, err := LoadOverrides(baseDir)
	if err != nil {
		t.Fatalf("LoadOverrides() error = %v", err)
	}

	hdfs := overrides.Profiles["hdfs"]
	if hdfs == nil || hdfs.Hadoop == nil {
		t.Fatal("expected hdfs Hadoop overrides")
	}
	if got := hdfs.Hadoop.CoreSite["hadoop.security.authorization"]; got != true {
		t.Errorf("core-site anchor value = %#v, want true", got)
	}
	if got := hdfs.Hadoop.HDFSSite["hadoop.tmp.dir"]; got != "/tmp/hadoop" {
		t.Errorf("hdfs-site alias value = %#v, want /tmp/hadoop", got)
	}
	if got := hdfs.Hadoop.YarnSite["yarn.nodemanager.resource.memory-mb"]; got != 4096 {
		t.Errorf("yarn-site mapping = %#v, want 4096", got)
	}
	if got := hdfs.Hadoop.MapredSite["mapreduce.framework.name"]; got != "yarn" {
		t.Errorf("mapred-site mapping = %#v, want yarn", got)
	}
	if got := hdfs.Hadoop.CapacityScheduler["yarn.scheduler.capacity.root.queues"]; got != "default" {
		t.Errorf("capacity-scheduler mapping = %#v, want default", got)
	}
	if got := hdfs.Hive["javax.jdo.option.ConnectionURL"]; got != "jdbc:postgresql://localhost/hive" {
		t.Errorf("hive mapping = %#v, want connection URL", got)
	}
	if got := hdfs.Spark["spark.sql.shuffle.partitions"]; got != 200 {
		t.Errorf("spark mapping = %#v, want 200", got)
	}
}

func TestLoadOverridesRejectsMalformedYAML(t *testing.T) {
	baseDir := writeOverrides(t, "profiles:\n  - malformed\n")

	_, err := LoadOverrides(baseDir)
	if err == nil {
		t.Fatal("LoadOverrides() error = nil, want malformed YAML error")
	}
	if !strings.Contains(err.Error(), "failed to parse overrides.yaml") {
		t.Errorf("LoadOverrides() error = %q, want parse context", err)
	}
}

func TestMergeOverridesConvertsScalarsToPropertyStrings(t *testing.T) {
	baseDir := writeOverrides(t, `profiles:
  local:
    hive:
      existing: new
      integer: 42
      float: 3.14
      boolean: true
`)
	overrides, err := LoadOverrides(baseDir)
	if err != nil {
		t.Fatalf("LoadOverrides() error = %v", err)
	}

	configSet := &schema.ConfigSet{
		Hive: &schema.HiveConfig{
			Extra: []schema.Property{{Name: "existing", Value: "old"}},
		},
	}

	merged := MergeOverrides(configSet, overrides.Profiles["local"])
	for name, want := range map[string]string{
		"existing": "new",
		"integer":  "42",
		"float":    "3.14",
		"boolean":  "true",
	} {
		if got := propertyValue(merged.Hive.Extra, name); got != want {
			t.Errorf("property %q = %q, want %q", name, got, want)
		}
	}
}

func writeOverrides(t *testing.T, contents string) string {
	t.Helper()
	baseDir := t.TempDir()
	overrideDir := filepath.Join(baseDir, "conf")
	if err := os.MkdirAll(overrideDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(overrideDir, "overrides.yaml"), []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return baseDir
}

func propertyValue(properties []schema.Property, name string) string {
	for _, property := range properties {
		if property.Name == name {
			return property.Value
		}
	}
	return ""
}
