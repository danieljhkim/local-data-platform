package env

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandUsesComputedPathInsteadOfParentPath(t *testing.T) {
	parentBin := t.TempDir()
	selectedBin := t.TempDir()
	writeExecutable(t, parentBin, "tool", "#!/bin/sh\nprintf parent\n")
	writeExecutable(t, selectedBin, "tool", "#!/bin/sh\nprintf 'selected:%s:' \"$1\"\nIFS= read -r input\nprintf %s \"$input\"\nprintf ':stderr' >&2\n")
	t.Setenv("PATH", parentBin)

	cmd, err := Command("tool", []string{"argument"}, []string{"PATH=" + selectedBin})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if got, want := cmd.Path, filepath.Join(selectedBin, "tool"); got != want {
		t.Fatalf("command path = %q, want selected installation %q", got, want)
	}

	cmd.Stdin = strings.NewReader("stdin")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got, want := stdout.String(), "selected:argument:stdin"; got != want {
		t.Errorf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), ":stderr"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
}

func TestCommandPreservesExplicitExecutablePath(t *testing.T) {
	dir := t.TempDir()
	path := writeExecutable(t, dir, "explicit-tool", "#!/bin/sh\nprintf explicit\n")

	cmd, err := Command(path, nil, []string{"PATH=" + t.TempDir()})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if got := cmd.Path; got != path {
		t.Fatalf("command path = %q, want explicit path %q", got, path)
	}
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Output() error = %v", err)
	}
	if got, want := string(output), "explicit"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestResolveExecutableReportsMissingAndNonExecutablePrograms(t *testing.T) {
	dir := t.TempDir()
	if _, err := ResolveExecutable("missing", []string{"PATH=" + dir}); !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("missing command error = %v, want exec.ErrNotFound", err)
	}

	path := filepath.Join(dir, "not-executable")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0644); err != nil {
		t.Fatalf("write non-executable = %v", err)
	}
	if _, err := ResolveExecutable("not-executable", []string{"PATH=" + dir}); err == nil || !strings.Contains(err.Error(), "not executable") {
		t.Errorf("non-executable command error = %v, want a non-executable error", err)
	}
}

func TestResolveExecutableUsesCurrentDirectoryForEmptyPathEntry(t *testing.T) {
	dir := t.TempDir()
	path := writeExecutable(t, dir, "current-dir-tool", "#!/bin/sh\n")
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })

	got, err := ResolveExecutable("current-dir-tool", []string{"PATH=:"})
	if err != nil {
		t.Fatalf("ResolveExecutable() error = %v", err)
	}
	if want := "./" + filepath.Base(path); got != want {
		t.Errorf("resolved path = %q, want %q", got, want)
	}
}

func writeExecutable(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0755); err != nil {
		t.Fatalf("write executable %s: %v", name, err)
	}
	return path
}
