package profile

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

const (
	cliSentinelCurrent   = "sentinel-secret-CURRENT"
	cliSentinelCandidate = "sentinel-secret-CANDIDATE"
)

func executeProfile(t *testing.T, paths *config.Paths, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewProfileCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), errBuf.String(), err
}

func TestProfileDiffCommand_UnknownProfile(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}

	_, _, err := executeProfile(t, paths, "diff", "missing")
	if err == nil {
		t.Fatal("expected unknown profile error")
	}
	if !strings.Contains(err.Error(), "unknown profile") {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(err.Error(), "Available profiles") {
		t.Fatalf("error should list available profiles: %v", err)
	}
}

func TestProfileDiffCommand_MissingOverlay(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}

	_, _, err := executeProfile(t, paths, "diff", "local")
	if err == nil {
		t.Fatal("expected missing overlay error")
	}
	if !strings.Contains(err.Error(), "runtime conf overlay not found") {
		t.Fatalf("error = %v", err)
	}
}

func TestProfileDiffCommand_RedactsSecrets(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, &generator.InitOptions{
		User:       "daniel",
		DBType:     "postgres",
		DBUrl:      "jdbc:postgresql://localhost:5432/metastore",
		DBPassword: "password",
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	currentHive := filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")
	if err := rewriteCLISecrets(currentHive, cliSentinelCurrent); err != nil {
		t.Fatalf("rewrite current: %v", err)
	}
	if err := rewriteCLISecrets(filepath.Join(paths.CurrentSparkConf(), "hive-site.xml"), cliSentinelCurrent); err != nil {
		t.Fatalf("rewrite spark hive-site: %v", err)
	}
	if err := rewriteCLISecrets(filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml"), cliSentinelCandidate); err != nil {
		t.Fatalf("rewrite candidate: %v", err)
	}

	out, errOut, err := executeProfile(t, paths, "diff", "local")
	if err != nil {
		t.Fatalf("profile diff: %v", err)
	}
	combined := out + errOut + errString(err)
	for _, secret := range []string{cliSentinelCurrent, cliSentinelCandidate} {
		if strings.Contains(combined, secret) {
			t.Fatalf("CLI output leaked %s:\nstdout:\n%s\nstderr:\n%s", secret, out, errOut)
		}
	}
	if !strings.Contains(out, util.RedactedValue) {
		t.Fatalf("expected redacted values:\n%s", out)
	}
}

func TestProfileDiffCommand_PreviewLocalVsHDFS(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	out, _, err := executeProfile(t, paths, "diff", "hdfs")
	if err != nil {
		t.Fatalf("profile diff hdfs: %v", err)
	}
	if !strings.Contains(out, "Files added:") {
		t.Fatalf("expected added files:\n%s", out)
	}
	if !strings.Contains(out, "hadoop/core-site.xml") {
		t.Fatalf("expected hadoop file addition:\n%s", out)
	}
}

func rewriteCLISecrets(path, secret string) error {
	cfg, err := util.ParseHadoopXML(path)
	if err != nil {
		return err
	}
	cfg.SetProperty(util.HiveConnectionPasswordProperty, secret)
	cfg.SetProperty("javax.jdo.option.ConnectionURL", "jdbc:postgresql://alice:"+secret+"@localhost:5432/metastore")
	return cfg.WriteXML(path)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
