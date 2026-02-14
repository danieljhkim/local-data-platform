package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func TestInit_ConfirmsEachMutableSetting(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	orig := runMetastoreBootstrap
	runMetastoreBootstrap = func(paths *config.Paths, in io.Reader, out, errOut io.Writer) error {
		return nil
	}
	defer func() { runMetastoreBootstrap = orig }()

	cmd := newInitCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetIn(strings.NewReader("\n\n\n\n"))

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "confirm user to be:") {
		t.Fatalf("missing user confirmation prompt:\n%s", output)
	}
	if !strings.Contains(output, "confirm db-type to be:") {
		t.Fatalf("missing db-type confirmation prompt:\n%s", output)
	}
	if !strings.Contains(output, "confirm db-url to be:") {
		t.Fatalf("missing db-url confirmation prompt:\n%s", output)
	}
	if !strings.Contains(output, "confirm db-password to be:") {
		t.Fatalf("missing db-password confirmation prompt:\n%s", output)
	}
	if strings.Contains(output, "base-dir") {
		t.Fatalf("base-dir should not be prompted for confirmation:\n%s", output)
	}
}

func TestInit_ConfirmationAllowsEditingValues(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	orig := runMetastoreBootstrap
	runMetastoreBootstrap = func(paths *config.Paths, in io.Reader, out, errOut io.Writer) error {
		return nil
	}
	defer func() { runMetastoreBootstrap = orig }()

	cmd := newInitCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetIn(strings.NewReader("\npostgres\njdbc:postgresql://edited-host:5432/edited_db\n\n"))

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}

	sm := config.NewSettingsManager(paths)
	settings, err := sm.Load()
	if err != nil {
		t.Fatalf("failed to load settings: %v", err)
	}

	if settings.DBType != "postgres" {
		t.Fatalf("DBType = %q", settings.DBType)
	}
	if settings.DBURL != "jdbc:postgresql://edited-host:5432/edited_db" {
		t.Fatalf("DBURL = %q", settings.DBURL)
	}
}

func TestInit_AlreadyInitializedWithoutForceReturnsWithoutConfirmations(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	if err := os.MkdirAll(filepath.Join(paths.UserProfilesDir(), "local"), 0755); err != nil {
		t.Fatalf("mkdir local profile: %v", err)
	}

	orig := runMetastoreBootstrap
	runMetastoreBootstrap = func(paths *config.Paths, in io.Reader, out, errOut io.Writer) error {
		t.Fatal("bootstrap should not run when already initialized without --force")
		return nil
	}
	defer func() { runMetastoreBootstrap = orig }()

	cmd := newInitCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetIn(strings.NewReader("\n\n\n\n"))

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}

	if !strings.Contains(errBuf.String(), "Profiles already initialized:") {
		t.Fatalf("missing already initialized notice:\n%s", errBuf.String())
	}
	if strings.Contains(out.String(), "confirm user to be:") {
		t.Fatalf("confirmation prompts should not be shown:\n%s", out.String())
	}
}

func TestInit_RejectsDBTypeDBURLMismatch(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	orig := runMetastoreBootstrap
	runMetastoreBootstrap = func(paths *config.Paths, in io.Reader, out, errOut io.Writer) error {
		t.Fatal("bootstrap should not run when validation fails")
		return nil
	}
	defer func() { runMetastoreBootstrap = orig }()

	cmd := newInitCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(errBuf)
	cmd.SetIn(strings.NewReader("\nmysql\njdbc:postgresql://localhost:5432/metastore\n\n"))

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "db-type and db-url must match") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(errBuf.String(), "WARNING:") {
		t.Fatalf("expected warning in stderr:\n%s", errBuf.String())
	}
}
