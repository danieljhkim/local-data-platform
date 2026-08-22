package setting

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
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

func TestSettingShow_Hive_RedactsConnectionPassword(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	hiveConfDir := paths.CurrentHiveConf()
	if err := os.MkdirAll(hiveConfDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	hivePath := filepath.Join(hiveConfDir, "hive-site.xml")
	content := `<configuration>
  <property><name>x</name><value>1</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>super-secret</value></property>
</configuration>
`
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
	if strings.Contains(out, "super-secret") {
		t.Fatalf("hive config output leaked password:\n%s", out)
	}
	if !strings.Contains(out, "<name>javax.jdo.option.ConnectionPassword</name>") || !strings.Contains(out, "<value>********</value>") {
		t.Fatalf("expected redacted password in output:\n%s", out)
	}
	if !strings.Contains(out, "<name>x</name>") || !strings.Contains(out, "<value>1</value>") {
		t.Fatalf("expected non-sensitive property in output:\n%s", out)
	}
}

func TestSettingShow_Spark_RedactsCopiedHiveSite(t *testing.T) {
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
	sparkHive := `<configuration>
  <property><name>hive.metastore.uris</name><value>thrift://localhost:9083</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>spark-secret</value></property>
</configuration>
`
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
	if strings.Contains(out, "spark-secret") {
		t.Fatalf("spark config output leaked password:\n%s", out)
	}
	if !strings.Contains(out, "<name>javax.jdo.option.ConnectionPassword</name>") || !strings.Contains(out, "<value>********</value>") {
		t.Fatalf("expected redacted password in output:\n%s", out)
	}
	if !strings.Contains(out, "<name>hive.metastore.uris</name>") || !strings.Contains(out, "<value>thrift://localhost:9083</value>") {
		t.Fatalf("expected non-sensitive hive-site property in output:\n%s", out)
	}
}

func TestSettingList_RedactsPasswordBearingDBURL(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	sm := config.NewSettingsManager(paths)
	if err := sm.Save(&config.Settings{
		User:       "daniel",
		DBType:     "postgres",
		DBURL:      "jdbc:postgresql://alice:" + secret + "@localhost:5432/metastore",
		DBPassword: secret,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"list"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting list: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, secret) {
		t.Fatalf("setting list leaked secret:\n%s", out)
	}
	if !strings.Contains(out, "db-url: jdbc:postgresql://alice:********@localhost:5432/metastore") {
		t.Fatalf("expected redacted db-url:\n%s", out)
	}
	if !strings.Contains(out, "db-password: ********") {
		t.Fatalf("expected masked db-password:\n%s", out)
	}
}

func TestSettingSet_DBPasswordFromStdin(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetIn(strings.NewReader(secret + "\n"))
	cmd.SetArgs([]string{"set", "db-password", "--stdin"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set: %v", err)
	}
	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("setting set leaked secret:\n%s", combined)
	}

	settings, err := config.NewSettingsManager(paths).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestSettingSet_DBPasswordFromFile(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	pwFile := filepath.Join(baseDir, "pw")
	if err := os.WriteFile(pwFile, []byte(secret+"\n"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-password", "--from-file", pwFile})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set: %v", err)
	}
	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("setting set leaked secret:\n%s", combined)
	}

	settings, err := config.NewSettingsManager(paths).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestSettingSet_DBPasswordPositionalIsDeprecated(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-password", secret})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set: %v", err)
	}
	if !strings.Contains(errBuf.String(), "deprecated") {
		t.Fatalf("expected deprecation warning, got: %s", errBuf.String())
	}
	if strings.Contains(out.String(), secret) {
		t.Fatalf("stdout leaked secret:\n%s", out.String())
	}

	settings, err := config.NewSettingsManager(paths).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestSettingSet_DBPasswordFromEnv(t *testing.T) {
	const secret = "s3cret-value"
	t.Setenv(util.DBPasswordEnvVar, secret)
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-password"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set: %v", err)
	}
	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("setting set leaked secret:\n%s", combined)
	}

	settings, err := config.NewSettingsManager(paths).Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestSettingSet_DBURLMismatchWarningRedactsPassword(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	sm := config.NewSettingsManager(paths)
	if err := sm.Save(&config.Settings{
		User:   "daniel",
		DBType: "postgres",
		DBURL:  "jdbc:postgresql://alice:" + secret + "@localhost:5432/metastore",
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	cmd := NewSettingCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs([]string{"set", "db-type", "mysql"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("setting set: %v", err)
	}
	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("warning leaked secret:\n%s", combined)
	}
}
