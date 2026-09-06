package wrappers

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
)

func TestBeelineArgs_UsesConfiguredDefaultURL(t *testing.T) {
	got := beelineArgs("jdbc:hive2://localhost:11000", []string{"-e", "SELECT 1"})
	want := []string{"beeline", "-u", "jdbc:hive2://localhost:11000", "-e", "SELECT 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("beelineArgs() = %#v, want %#v", got, want)
	}
}

func TestBeelineArgs_ExplicitURLTakesPrecedence(t *testing.T) {
	for _, args := range [][]string{
		{"-u", "jdbc:hive2://127.0.0.1:12000", "-e", "SELECT 1"},
		{"--url=jdbc:hive2://127.0.0.1:12000", "-e", "SELECT 1"},
	} {
		got := beelineArgs("jdbc:hive2://localhost:11000", args)
		want := append([]string{"beeline"}, args...)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("beelineArgs(%#v) = %#v, want %#v", args, got, want)
		}
	}
}

func TestRunHiveLaunchesFromURLSnapshot(t *testing.T) {
	confDir := t.TempDir()
	writeHiveSite(t, confDir, 11000)
	prepared := &envpkg.Environment{
		ActiveProfile: "hdfs",
		HiveConfDir:   confDir,
	}
	newer := &envpkg.Environment{
		ActiveProfile: "local",
		HiveConfDir:   t.TempDir(),
	}
	writeHiveSite(t, newer.HiveConfDir, 12000)

	published := prepared
	computeCalls := 0
	var launched *envpkg.Environment
	var launchedArgs []string
	var launchedExtra map[string]string
	err := runHive(nil, []string{"-e", "SELECT 1"}, func(_ *config.Paths) (*envpkg.Environment, error) {
		computeCalls++
		return prepared, nil
	}, func(environment *envpkg.Environment, args []string, extra map[string]string) error {
		// A profile switch after URL preparation must not replace the launch
		// environment. The executor receives the prepared snapshot directly.
		published = newer
		launched = environment
		launchedArgs = args
		launchedExtra = extra
		return nil
	})
	if err != nil {
		t.Fatalf("runHive() error = %v", err)
	}
	if computeCalls != 1 {
		t.Fatalf("Compute calls = %d, want 1", computeCalls)
	}
	if published != newer {
		t.Fatal("simulated profile switch did not publish the newer environment")
	}
	if launched != prepared {
		t.Fatalf("launch environment = %#v, want prepared snapshot %#v", launched, prepared)
	}
	wantArgs := []string{"beeline", "-u", "jdbc:hive2://localhost:11000", "-e", "SELECT 1"}
	if !reflect.DeepEqual(launchedArgs, wantArgs) {
		t.Fatalf("launch args = %#v, want %#v", launchedArgs, wantArgs)
	}
	if got := launchedExtra["TERM"]; got != "dumb" {
		t.Fatalf("TERM = %q, want dumb", got)
	}
}

func writeHiveSite(t *testing.T, dir string, port int) {
	t.Helper()
	contents := "<configuration><property><name>hive.server2.thrift.port</name><value>" + strconv.Itoa(port) + "</value></property></configuration>"
	if err := os.WriteFile(filepath.Join(dir, "hive-site.xml"), []byte(contents), 0644); err != nil {
		t.Fatalf("write hive-site.xml: %v", err)
	}
}
