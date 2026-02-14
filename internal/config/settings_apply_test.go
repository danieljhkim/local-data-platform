package config

import (
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

func TestSettingsApply_DBURLAndPasswordAndUser(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)

	if err := pm.Init(false, &generator.InitOptions{
		DBType:     "postgres",
		DBUrl:      "jdbc:postgresql://localhost:5432/metastore",
		DBPassword: "password",
		User:       "old-user",
	}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("set hdfs: %v", err)
	}

	applier := NewSettingsApplier(paths)

	if err := applier.Apply("db-url", "jdbc:postgresql://localhost:5432/metastore", "jdbc:postgresql://new-host:5432/newdb"); err != nil {
		t.Fatalf("apply db-url: %v", err)
	}
	if err := applier.Apply("db-password", "password", "new-secret"); err != nil {
		t.Fatalf("apply db-password: %v", err)
	}
	if err := applier.Apply("user", "old-user", "new-user"); err != nil {
		t.Fatalf("apply user: %v", err)
	}

	checkHive := func(path string) {
		cfg, err := util.ParseHadoopXML(path)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if got := cfg.GetProperty("javax.jdo.option.ConnectionURL"); got != "jdbc:postgresql://new-host:5432/newdb" {
			t.Fatalf("%s ConnectionURL = %q", path, got)
		}
		if got := cfg.GetProperty("javax.jdo.option.ConnectionPassword"); got != "new-secret" {
			t.Fatalf("%s ConnectionPassword = %q", path, got)
		}
		if got := cfg.GetProperty("javax.jdo.option.ConnectionUserName"); got != "new-user" {
			t.Fatalf("%s ConnectionUserName = %q", path, got)
		}
	}

	checkHive(filepath.Join(paths.UserProfilesDir(), "hdfs", "hive", "hive-site.xml"))
	checkHive(filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml"))
	checkHive(filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"))
	checkHive(filepath.Join(paths.CurrentSparkConf(), "hive-site.xml"))
}

func TestSettingsApply_BaseDirIsFutureOnly(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)

	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("set hdfs: %v", err)
	}

	hdfsCore := filepath.Join(paths.UserProfilesDir(), "hdfs", "hadoop", "core-site.xml")
	cfgBefore, err := util.ParseHadoopXML(hdfsCore)
	if err != nil {
		t.Fatalf("parse before: %v", err)
	}
	before := cfgBefore.GetProperty("hadoop.tmp.dir")

	applier := NewSettingsApplier(paths)
	if err := applier.Apply("base-dir", paths.BaseDir, filepath.Join(tmpDir, "new-base")); err != nil {
		t.Fatalf("apply base-dir: %v", err)
	}

	cfgAfter, err := util.ParseHadoopXML(hdfsCore)
	if err != nil {
		t.Fatalf("parse after: %v", err)
	}
	after := cfgAfter.GetProperty("hadoop.tmp.dir")

	if after != before {
		t.Fatalf("base-dir apply should not mutate existing config: before=%q after=%q", before, after)
	}
}

func TestSettingsApply_MissingFilesNoError(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	applier := NewSettingsApplier(paths)

	if err := applier.Apply("db-url", "old", "new"); err != nil {
		t.Fatalf("expected no error when files missing: %v", err)
	}
}

func TestSettingsApply_UserStaysAPPForDerby(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)

	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("set hdfs: %v", err)
	}

	applier := NewSettingsApplier(paths)
	if err := applier.Apply("user", "old-user", "new-user"); err != nil {
		t.Fatalf("apply user: %v", err)
	}

	cfg, err := util.ParseHadoopXML(filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"))
	if err != nil {
		t.Fatalf("parse hive-site: %v", err)
	}
	if got := cfg.GetProperty("javax.jdo.option.ConnectionUserName"); got != "APP" {
		t.Fatalf("ConnectionUserName = %q, want APP", got)
	}
}

func TestSettingsApply_DBTypeUpdatesDriverAndURL(t *testing.T) {
	tmpDir := t.TempDir()
	paths := NewPaths(filepath.Join(tmpDir, "repo"), filepath.Join(tmpDir, "base"))
	pm := NewProfileManager(paths)

	if err := pm.Init(false, nil); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("set hdfs: %v", err)
	}

	sm := NewSettingsManager(paths)
	if err := sm.Save(&Settings{
		User:       "daniel",
		DBType:     "postgres",
		DBURL:      "jdbc:postgresql://localhost:5432/metastore",
		DBPassword: "password",
	}); err != nil {
		t.Fatalf("save settings: %v", err)
	}

	applier := NewSettingsApplier(paths)
	if err := applier.Apply("db-type", "derby", "postgres"); err != nil {
		t.Fatalf("apply db-type: %v", err)
	}

	checkHive := func(path string) {
		cfg, err := util.ParseHadoopXML(path)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if got := cfg.GetProperty("javax.jdo.option.ConnectionDriverName"); got != "org.postgresql.Driver" {
			t.Fatalf("%s ConnectionDriverName = %q", path, got)
		}
		if got := cfg.GetProperty("javax.jdo.option.ConnectionURL"); got != "jdbc:postgresql://localhost:5432/metastore" {
			t.Fatalf("%s ConnectionURL = %q", path, got)
		}
	}

	checkHive(filepath.Join(paths.UserProfilesDir(), "hdfs", "hive", "hive-site.xml"))
	checkHive(filepath.Join(paths.UserProfilesDir(), "local", "hive", "hive-site.xml"))
	checkHive(filepath.Join(paths.CurrentHiveConf(), "hive-site.xml"))
	checkHive(filepath.Join(paths.CurrentSparkConf(), "hive-site.xml"))
}
