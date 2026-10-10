package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
)

func TestRepoRootAndComputedPathIgnoreWorkingDirectoryConf(t *testing.T) {
	tests := []struct {
		name string
		cwd  func(string) string
	}{
		{
			name: "cwd contains conf",
			cwd:  func(projectRoot string) string { return projectRoot },
		},
		{
			name: "parent contains conf",
			cwd: func(projectRoot string) string {
				return filepath.Join(projectRoot, "child")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			projectRoot := t.TempDir()
			if err := os.MkdirAll(filepath.Join(projectRoot, "conf"), 0755); err != nil {
				t.Fatalf("create project conf directory: %v", err)
			}
			projectBin := filepath.Join(projectRoot, "bin")
			if err := os.MkdirAll(projectBin, 0755); err != nil {
				t.Fatalf("create project bin directory: %v", err)
			}
			writeTestExecutable(t, projectBin, "beeline")

			cwd := tt.cwd(projectRoot)
			if err := os.MkdirAll(cwd, 0755); err != nil {
				t.Fatalf("create cwd: %v", err)
			}
			previousCWD, err := os.Getwd()
			if err != nil {
				t.Fatalf("get current working directory: %v", err)
			}
			if err := os.Chdir(cwd); err != nil {
				t.Fatalf("change working directory: %v", err)
			}
			t.Cleanup(func() {
				if err := os.Chdir(previousCWD); err != nil {
					t.Errorf("restore working directory: %v", err)
				}
			})

			// Model an installed executable outside the untrusted project.
			installBin := filepath.Join(t.TempDir(), "install", "bin")
			if err := os.MkdirAll(installBin, 0755); err != nil {
				t.Fatalf("create installed binary directory: %v", err)
			}
			if got := repoRootFromExecutable(filepath.Join(installBin, "local-data")); got != "" {
				t.Fatalf("repository root from unrelated executable = %q, want empty", got)
			}

			// Exercise the same computation and resolution path used by wrappers,
			// services, and Hive schema commands. PATH contains only absolute dirs.
			baseDir := t.TempDir()
			hiveHome := t.TempDir()
			configuredBin := filepath.Join(hiveHome, "bin")
			if err := os.MkdirAll(configuredBin, 0755); err != nil {
				t.Fatalf("create configured Hive bin directory: %v", err)
			}
			configuredBeeline := writeTestExecutable(t, configuredBin, "beeline")
			inheritedBin := t.TempDir()

			t.Setenv("HIVE_HOME", hiveHome)
			t.Setenv("SPARK_HOME", t.TempDir())
			t.Setenv("HADOOP_HOME", "")
			t.Setenv("PATH", inheritedBin)

			paths := config.NewPaths(repoRootFromExecutable(filepath.Join(installBin, "local-data")), baseDir)
			if err := config.NewProfileManager(paths).Init(false, nil); err != nil {
				t.Fatalf("initialize test profiles: %v", err)
			}
			computed, err := envpkg.Compute(paths)
			if err != nil {
				t.Fatalf("compute environment: %v", err)
			}
			cmd, err := envpkg.Command("beeline", nil, computed.Export())
			if err != nil {
				t.Fatalf("construct beeline command: %v", err)
			}
			if cmd.Path != configuredBeeline {
				t.Fatalf("resolved beeline = %q, want configured installation %q", cmd.Path, configuredBeeline)
			}
			for _, entry := range filepath.SplitList(computed.Path) {
				if entry == projectBin {
					t.Fatalf("computed PATH still contains untrusted project bin: %q", computed.Path)
				}
			}
		})
	}
}

func TestRepoRootStillDiscoversExecutableRelativeCheckout(t *testing.T) {
	projectRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(projectRoot, "conf"), 0755); err != nil {
		t.Fatalf("create checkout conf directory: %v", err)
	}
	bin := filepath.Join(projectRoot, "bin")
	if err := os.MkdirAll(bin, 0755); err != nil {
		t.Fatalf("create checkout bin directory: %v", err)
	}

	if got, want := repoRootFromExecutable(filepath.Join(bin, "local-data")), projectRoot; got != want {
		t.Fatalf("repository root = %q, want executable-relative checkout %q", got, want)
	}
}

func writeTestExecutable(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
	return path
}
