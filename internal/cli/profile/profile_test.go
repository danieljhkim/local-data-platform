package profile

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

func TestProfileCommand_DoesNotExposeInitSubcommand(t *testing.T) {
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)

	cmd := NewProfileCmd(func() *config.Paths { return paths })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"init"})

	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected unknown command error for profile init")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProfileSetCommand_RepairsMissingActiveOverlay(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	if err := os.RemoveAll(paths.CurrentConfDir()); err != nil {
		t.Fatalf("remove overlay: %v", err)
	}
	if _, _, err := executeProfile(t, paths, "check"); err == nil {
		t.Fatal("expected profile check to report the missing overlay")
	}

	if _, _, err := executeProfile(t, paths, "set", "local"); err != nil {
		t.Fatalf("profile set should repair the active profile: %v", err)
	}
	if err := pm.Check(); err != nil {
		t.Fatalf("repaired overlay should pass profile check: %v", err)
	}
	if active, err := paths.ActiveProfile(); err != nil || active != "local" {
		t.Fatalf("active profile = %q, err = %v, want local", active, err)
	}
}

func TestProfileSetCommand_RematerializesChangedActiveProfile(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	source := filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml")
	cfg, err := util.ParseHadoopXML(source)
	if err != nil {
		t.Fatalf("parse source: %v", err)
	}
	const changedWarehouse = "file:///tmp/profile-set-command-test-warehouse"
	cfg.SetProperty("hive.metastore.warehouse.dir", changedWarehouse)
	if err := cfg.WriteXML(source); err != nil {
		t.Fatalf("change source: %v", err)
	}

	if _, _, err := executeProfile(t, paths, "set", "local"); err != nil {
		t.Fatalf("profile set after source change: %v", err)
	}
	materialized, err := util.ParseHadoopXML(filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"))
	if err != nil {
		t.Fatalf("parse materialized overlay: %v", err)
	}
	if got := materialized.GetProperty("hive.metastore.warehouse.dir"); got != changedWarehouse {
		t.Fatalf("materialized warehouse dir = %q, want %q", got, changedWarehouse)
	}
}

func TestProfileSetCommand_PreservesOverlayOnInvalidActiveProfile(t *testing.T) {
	tmpDir := t.TempDir()
	paths := config.NewPaths("", tmpDir)
	pm := config.NewProfileManager(paths)
	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("local"); err != nil {
		t.Fatalf("set local: %v", err)
	}

	current := filepath.Join(paths.CurrentHiveConf(), "hive-site.xml")
	before, err := os.ReadFile(current)
	if err != nil {
		t.Fatalf("read current overlay: %v", err)
	}
	markerBefore, err := os.ReadFile(paths.ActiveProfileFile())
	if err != nil {
		t.Fatalf("read active marker: %v", err)
	}
	source := filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml")
	if err := os.Remove(source); err != nil {
		t.Fatalf("remove required source config: %v", err)
	}

	if _, _, err := executeProfile(t, paths, "set", "local"); err == nil {
		t.Fatal("expected profile set to reject invalid source")
	}
	after, err := os.ReadFile(current)
	if err != nil {
		t.Fatalf("read overlay after failed set: %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("failed profile set changed the existing overlay")
	}
	markerAfter, err := os.ReadFile(paths.ActiveProfileFile())
	if err != nil {
		t.Fatalf("read marker after failed set: %v", err)
	}
	if !bytes.Equal(markerAfter, markerBefore) {
		t.Fatalf("failed profile set changed active marker from %q to %q", markerBefore, markerAfter)
	}
}
