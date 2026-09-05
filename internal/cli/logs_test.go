package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func writeLogFixture(t *testing.T, paths *config.Paths, service, name, content string) string {
	t.Helper()
	dir := paths.ServiceStateDir(service).LogsDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestCollectLogs_RejectsUnknownServiceBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	if _, err := collectLogs(paths, "hdfs", "bogus", defaultLogLines); err == nil {
		t.Fatal("expected error for unknown service")
	}
}

func TestCollectLogs_RejectsInvalidLinesBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	// Write a real file so a failure to validate-before-read would be visible
	// via unexpected file collection.
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\n")

	if _, err := collectLogs(paths, "local", "hive", -1); err == nil {
		t.Fatal("expected error for negative --lines")
	}
	if _, err := collectLogs(paths, "local", "hive", maxLogLines+1); err == nil {
		t.Fatal("expected error for --lines exceeding max")
	}
}

func TestCollectLogs_LocalProfileDefaultsToHiveOnly(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "local", "", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 1 || report.Services[0].Name != "hive" {
		t.Fatalf("report.Services = %#v, want only hive", report.Services)
	}
}

func TestCollectLogs_HdfsProfileDefaultsToAllServices(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "hdfs", "", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 3 {
		t.Fatalf("report.Services = %#v, want hdfs+yarn+hive", report.Services)
	}
}

func TestCollectLogs_ExplicitSelectionIgnoresActiveProfile(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	report, err := collectLogs(paths, "local", "hdfs", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Services) != 1 || report.Services[0].Name != "hdfs" {
		t.Fatalf("report.Services = %#v, want only hdfs despite local profile", report.Services)
	}
}

func TestCollectLogs_UnknownExplicitServiceIsRejected(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	if _, err := collectLogs(paths, "hdfs", "spark", defaultLogLines); err == nil {
		t.Fatal("expected error for unsupported explicit service")
	}
}

func TestCollectLogs_TailsMultilineFileToLastNLines(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "l1\nl2\nl3\nl4\nl5\n")

	report, err := collectLogs(paths, "local", "hive", 2)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "l4,l5" {
		t.Fatalf("lines = %#v, want [l4 l5]", got)
	}
}

func TestCollectLogs_NoFinalNewlinePreservesLastLine(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc")

	report, err := collectLogs(paths, "local", "hive", 10)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("lines = %#v, want [a b c]", got)
	}
}

func TestCollectLogs_EmptyFileProducesNoLinesAndNoError(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "")

	report, err := collectLogs(paths, "local", "hive", 10)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	f := report.Services[0].Files[0]
	if f.Missing || f.Error != "" || len(f.Lines) != 0 {
		t.Fatalf("file = %#v, want present/no-error/no-lines", f)
	}
}

func TestCollectLogs_ZeroLinesSkipsReadingContent(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc\n")

	report, err := collectLogs(paths, "local", "hive", 0)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	f := report.Services[0].Files[0]
	if f.Missing || f.Error != "" || len(f.Lines) != 0 {
		t.Fatalf("file = %#v, want present/no-error/no-lines for --lines 0", f)
	}
}

func TestCollectLogs_LargeCountReturnsWholeFile(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "a\nb\nc\n")

	report, err := collectLogs(paths, "local", "hive", maxLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	got := report.Services[0].Files[0].Lines
	if strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("lines = %#v, want [a b c]", got)
	}
}

func TestCollectLogs_MissingFileIsNotAnError(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	// Neither metastore.log nor hiveserver2.log exists.
	report, err := collectLogs(paths, "local", "hive", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs: %v", err)
	}
	if len(report.Errors) != 0 {
		t.Fatalf("errors = %#v, want none for missing files", report.Errors)
	}
	for _, f := range report.Services[0].Files {
		if !f.Missing {
			t.Fatalf("file = %#v, want missing=true", f)
		}
	}
}

func TestCollectLogs_ReadErrorDoesNotSuppressOtherFiles(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	dir := paths.ServiceStateDir("hive").LogsDir
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// metastore.log is a directory, not a readable log file.
	if err := os.MkdirAll(filepath.Join(dir, "metastore.log"), 0755); err != nil {
		t.Fatalf("mkdir metastore.log: %v", err)
	}
	writeLogFixture(t, paths, "hive", "hiveserver2.log", "ready\n")

	report, err := collectLogs(paths, "local", "hive", defaultLogLines)
	if err != nil {
		t.Fatalf("collectLogs returned unexpected top-level error: %v", err)
	}
	if len(report.Errors) != 1 {
		t.Fatalf("errors = %#v, want exactly one", report.Errors)
	}

	var hs2 *logsFile
	for i := range report.Services[0].Files {
		if strings.HasSuffix(report.Services[0].Files[i].Path, "hiveserver2.log") {
			hs2 = &report.Services[0].Files[i]
		}
	}
	if hs2 == nil {
		t.Fatal("hiveserver2.log entry not found in report")
	}
	if hs2.Error != "" || len(hs2.Lines) != 1 || hs2.Lines[0] != "ready" {
		t.Fatalf("hiveserver2.log entry = %#v, want unaffected by metastore.log error", hs2)
	}
}

func TestLogsCmd_RejectsInvalidLinesFlagBeforeReading(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	cmd := NewLogsCmd(func() *config.Paths { return paths })
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive", "--lines", "-5"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for negative --lines")
	}
}

func TestLogsCmd_RejectsUnknownService(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	cmd := NewLogsCmd(func() *config.Paths { return paths })
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"bogus"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for unknown service")
	}
}

func TestLogsCmd_PrintsDeterministicSourceLabelsAndTail(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	writeLogFixture(t, paths, "hive", "metastore.log", "one\ntwo\nthree\n")

	cmd := NewLogsCmd(func() *config.Paths { return paths })
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive", "--lines", "2"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("logs hive: %v", err)
	}

	metastorePath := filepath.Join(paths.ServiceStateDir("hive").LogsDir, "metastore.log")
	if !strings.Contains(out.String(), "==> "+metastorePath+" <==") {
		t.Fatalf("missing deterministic source label:\n%s", out.String())
	}
	if strings.Contains(out.String(), "one") {
		t.Fatalf("expected tail to exclude line beyond --lines 2:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "two") || !strings.Contains(out.String(), "three") {
		t.Fatalf("expected last 2 lines present:\n%s", out.String())
	}
}

func TestLogsCmd_ReadFailureReturnsNonzeroExitButKeepsOtherOutput(t *testing.T) {
	paths := &config.Paths{BaseDir: t.TempDir()}
	dir := paths.ServiceStateDir("hive").LogsDir
	if err := os.MkdirAll(filepath.Join(dir, "metastore.log"), 0755); err != nil {
		t.Fatalf("mkdir metastore.log: %v", err)
	}
	writeLogFixture(t, paths, "hive", "hiveserver2.log", "ready\n")

	cmd := NewLogsCmd(func() *config.Paths { return paths })
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"hive"})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected nonzero exit for read failure")
	}
	if !strings.Contains(out.String(), "ready") {
		t.Fatalf("expected hiveserver2.log output despite metastore.log read error:\n%s", out.String())
	}
}
