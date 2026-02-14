package profile

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

func TestProfileInit_ConfirmsEachMutableSetting(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewProfileCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetIn(strings.NewReader("\n\n\n"))
	cmd.SetArgs([]string{"init"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("profile init returned error: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "confirm user to be:") {
		t.Fatalf("missing user confirmation prompt:\n%s", output)
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

func TestProfileInit_ConfirmationAllowsEditingValues(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewProfileCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetIn(strings.NewReader("\njdbc:postgresql://edited-host:5432/edited_db\n\n"))
	cmd.SetArgs([]string{"init"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("profile init returned error: %v", err)
	}

	sm := config.NewSettingsManager(paths)
	settings, err := sm.Load()
	if err != nil {
		t.Fatalf("failed to load settings: %v", err)
	}

	if settings.DBURL != "jdbc:postgresql://edited-host:5432/edited_db" {
		t.Fatalf("DBURL = %q", settings.DBURL)
	}
}

func TestProfileInit_AlreadyInitializedWithoutForceReturnsWithoutConfirmations(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	if err := os.MkdirAll(filepath.Join(paths.UserProfilesDir(), "local"), 0755); err != nil {
		t.Fatalf("mkdir local profile: %v", err)
	}

	cmd := NewProfileCmd(func() *config.Paths { return paths })
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetIn(strings.NewReader("\n\n\n"))
	cmd.SetArgs([]string{"init"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("profile init returned error: %v", err)
	}

	output := out.String()
	initializedIdx := strings.Index(output, "Profiles already initialized:")
	confirmIdx := strings.Index(output, "confirm user to be:")
	if initializedIdx == -1 {
		t.Fatalf("missing already initialized notice:\n%s", output)
	}
	if confirmIdx != -1 {
		t.Fatalf("confirmation prompts should not be shown when already initialized without --force:\n%s", output)
	}
}
