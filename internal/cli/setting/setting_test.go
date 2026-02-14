package setting

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func executeCommand(t *testing.T, cmdArgs ...string) (string, error) {
	t.Helper()

	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(cmdArgs)

	err := cmd.Execute()
	return buf.String(), err
}

func TestSettingList_PrintsAllConfigurableKeys(t *testing.T) {
	out, err := executeCommand(t, "list")
	if err != nil {
		t.Fatalf("setting list returned error: %v", err)
	}

	if !strings.Contains(out, "- user: ") {
		t.Fatalf("output missing user key:\n%s", out)
	}
	if !strings.Contains(out, "- base-dir: ") {
		t.Fatalf("output missing base-dir key:\n%s", out)
	}
	if !strings.Contains(out, "- db-type: ") {
		t.Fatalf("output missing db-type key:\n%s", out)
	}
	if !strings.Contains(out, "- db-url: ") {
		t.Fatalf("output missing db-url key:\n%s", out)
	}
	if !strings.Contains(out, "- db-password: ********") {
		t.Fatalf("output should mask db-password:\n%s", out)
	}
}

func TestSettingSet_UpdatesValueInSettingsFile(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-type", "postgres"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set returned error: %v", err)
	}

	sm := config.NewSettingsManager(paths)
	settings, err := sm.Load()
	if err != nil {
		t.Fatalf("failed to load settings: %v", err)
	}

	if settings.DBType != "postgres" {
		t.Fatalf("DBType = %q", settings.DBType)
	}
	if settings.DBURL != "jdbc:postgresql://localhost:5432/metastore" {
		t.Fatalf("DBURL = %q", settings.DBURL)
	}
	if !strings.Contains(errBuf.String(), "Run 'local-data init --force'") {
		t.Fatalf("expected profile init warning, got: %s", errBuf.String())
	}
}

func TestSettingSet_DBURLMismatchIsRejected(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-url", "jdbc:postgresql://new-host:5432/newdb"})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "db-type and db-url must match") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(errBuf.String(), "WARNING:") {
		t.Fatalf("expected warning, got: %s", errBuf.String())
	}
}

func TestSettingSet_RejectsUnknownKey(t *testing.T) {
	_, err := executeCommand(t, "set", "unknown", "value")
	if err == nil {
		t.Fatalf("expected error for unknown key")
	}
	if !strings.Contains(err.Error(), "unknown setting key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSettingSet_BaseDirIsNotEditable(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"set", "base-dir", filepath.Join(baseDir, "new-base")})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected error when setting base-dir")
	}
	if !strings.Contains(err.Error(), "base-dir is static and cannot be changed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSettingShow_Hive_PrintsActiveConfig(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	hiveConfDir := paths.CurrentHiveConf()
	if err := os.MkdirAll(hiveConfDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hivePath := filepath.Join(hiveConfDir, "hive-site.xml")
	content := "<configuration><property><name>x</name><value>1</value></property></configuration>\n"
	if err := os.WriteFile(hivePath, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"show", "hive"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting show hive returned error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, content) {
		t.Fatalf("expected hive config content in output:\n%s", out)
	}
}

func TestSettingShow_Spark_PrintsSparkDefaultsAndHiveSite(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	sparkConfDir := paths.CurrentSparkConf()
	if err := os.MkdirAll(sparkConfDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	sparkDefaultsPath := filepath.Join(sparkConfDir, "spark-defaults.conf")
	sparkDefaults := "spark.master local[*]\n"
	if err := os.WriteFile(sparkDefaultsPath, []byte(sparkDefaults), 0644); err != nil {
		t.Fatalf("write spark-defaults: %v", err)
	}
	sparkHivePath := filepath.Join(sparkConfDir, "hive-site.xml")
	sparkHive := "<configuration><property><name>hive.metastore.uris</name><value>thrift://localhost:9083</value></property></configuration>\n"
	if err := os.WriteFile(sparkHivePath, []byte(sparkHive), 0644); err != nil {
		t.Fatalf("write hive-site: %v", err)
	}

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"show", "spark"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting show spark returned error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, sparkDefaults) {
		t.Fatalf("expected spark-defaults content in output:\n%s", out)
	}
	if !strings.Contains(out, sparkHive) {
		t.Fatalf("expected spark hive-site content in output:\n%s", out)
	}
}
