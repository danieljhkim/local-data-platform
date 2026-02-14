package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSettingsManager_Path(t *testing.T) {
	baseDir := t.TempDir()
	paths := NewPaths("/tmp/repo", baseDir)
	sm := NewSettingsManager(paths)

	want := filepath.Join(baseDir, "settings", "setting.json")
	if got := sm.Path(); got != want {
		t.Fatalf("Path() = %q, want %q", got, want)
	}
}

func TestSettingsManager_LoadOrDefault_MissingFile(t *testing.T) {
	baseDir := t.TempDir()
	paths := NewPaths("/tmp/repo", baseDir)
	sm := NewSettingsManager(paths)

	got, err := sm.LoadOrDefault()
	if err != nil {
		t.Fatalf("LoadOrDefault() error: %v", err)
	}

	if got.BaseDir != baseDir {
		t.Errorf("BaseDir = %q, want %q", got.BaseDir, baseDir)
	}
	if got.DBURL != "jdbc:postgresql://localhost:5432/metastore" {
		t.Errorf("DBURL = %q", got.DBURL)
	}
	if got.DBPassword != "password" {
		t.Errorf("DBPassword = %q", got.DBPassword)
	}
	if strings.TrimSpace(got.User) == "" {
		t.Errorf("User should not be empty")
	}
}

func TestSettingsManager_SaveAndLoad(t *testing.T) {
	baseDir := t.TempDir()
	paths := NewPaths("/tmp/repo", baseDir)
	sm := NewSettingsManager(paths)

	want := &Settings{
		User:       "daniel",
		BaseDir:    "/tmp/custom-base",
		DBURL:      "jdbc:postgresql://localhost:5432/custom",
		DBPassword: "secret",
	}

	if err := sm.Save(want); err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	if _, err := os.Stat(filepath.Dir(sm.Path())); err != nil {
		t.Fatalf("settings parent dir should exist: %v", err)
	}

	got, err := sm.Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if *got != *want {
		t.Fatalf("Load() = %+v, want %+v", *got, *want)
	}
}

func TestSettingsManager_Load_InvalidJSON(t *testing.T) {
	baseDir := t.TempDir()
	paths := NewPaths("/tmp/repo", baseDir)
	sm := NewSettingsManager(paths)

	if err := os.MkdirAll(filepath.Dir(sm.Path()), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(sm.Path(), []byte("{invalid-json"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := sm.Load(); err == nil {
		t.Fatalf("Load() expected error for invalid JSON")
	}
}
