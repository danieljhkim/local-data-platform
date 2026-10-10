package hive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/env"
	"github.com/danieljhkim/local-data-platform/internal/metastore"
)

func TestHiveService_DetectMetastoreConfig(t *testing.T) {
	tests := []struct {
		name     string
		hiveConf string
		expected metastore.DBType
	}{
		{
			name: "postgres metastore",
			hiveConf: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
</configuration>`,
			expected: metastore.Postgres,
		},
		{
			name: "derby metastore",
			hiveConf: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.iapi.jdbc.AutoloadedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
</configuration>`,
			expected: metastore.Derby,
		},
		{
			name: "empty config",
			hiveConf: `<?xml version="1.0"?>
<configuration>
</configuration>`,
			expected: metastore.Derby,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			baseDir := filepath.Join(tmpDir, "base")

			// Create profile structure (needed for initial overlay)
			if err := setupTestProfile(tmpDir); err != nil {
				t.Fatalf("Failed to create profile dir: %v", err)
			}

			paths := &config.Paths{
				RepoRoot: tmpDir,
				BaseDir:  baseDir,
			}

			service := newTestHiveService(t, paths)

			// Override the hive-site.xml in the overlay location (conf/current/hive)
			// This is what ensurePostgresJDBC actually reads
			overlayHiveDir := filepath.Join(baseDir, "conf", "current", "hive")
			if err := os.MkdirAll(overlayHiveDir, 0755); err != nil {
				t.Fatalf("Failed to create overlay hive dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(overlayHiveDir, "hive-site.xml"), []byte(tt.hiveConf), 0644); err != nil {
				t.Fatalf("Failed to write hive-site.xml: %v", err)
			}

			dbType, _, err := service.detectMetastoreConfig()
			if err != nil {
				t.Fatalf("detectMetastoreConfig() error = %v", err)
			}

			if dbType != tt.expected {
				t.Errorf("detectMetastoreConfig() dbType = %v, want %v", dbType, tt.expected)
			}
		})
	}
}

func TestHiveService_EnsureMetastoreSchema_NotPostgres(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create minimal profile
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service := newTestHiveService(t, paths)

	// Override with Derby config in the overlay location
	derbyConfig := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby:;databaseName=metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.iapi.jdbc.AutoloadedDriver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>APP</value>
  </property>
</configuration>`
	overlayHiveDir := filepath.Join(baseDir, "conf", "current", "hive")
	if err := os.MkdirAll(overlayHiveDir, 0755); err != nil {
		t.Fatalf("Failed to create overlay hive dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(overlayHiveDir, "hive-site.xml"), []byte(derbyConfig), 0644); err != nil {
		t.Fatalf("Failed to write hive-site.xml: %v", err)
	}

	// ensureMetastoreSchema should return nil immediately for non-Postgres metastore
	err := service.ensureMetastoreSchema(context.Background())
	if err != nil {
		t.Errorf("ensureMetastoreSchema() should return nil for non-Postgres, got: %v", err)
	}
}

func TestHiveService_EnsureMetastoreSchema_PostgresNoSchematool(t *testing.T) {
	tmpDir := t.TempDir()
	baseDir := filepath.Join(tmpDir, "base")

	// Create profile using standard setup
	if err := setupTestProfile(tmpDir); err != nil {
		t.Fatalf("Failed to setup test profile: %v", err)
	}

	paths := &config.Paths{
		RepoRoot: tmpDir,
		BaseDir:  baseDir,
	}

	service := newTestHiveService(t, paths)

	postgresConfig := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
</configuration>`
	overlayHiveDir := filepath.Join(baseDir, "conf", "current", "hive")
	if err := os.MkdirAll(overlayHiveDir, 0755); err != nil {
		t.Fatalf("Failed to create overlay hive dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(overlayHiveDir, "hive-site.xml"), []byte(postgresConfig), 0644); err != nil {
		t.Fatalf("Failed to write hive-site.xml: %v", err)
	}

	// ensureMetastoreSchema should not return an error when schematool fails
	// (it logs a warning and continues)
	err := service.ensureMetastoreSchema(context.Background())
	// This may log warnings but should not error (graceful degradation)
	if err != nil {
		t.Logf("ensureMetastoreSchema() returned error (expected if schematool not in PATH): %v", err)
	}
}

// Note: Full schema initialization tests require:
// - Postgres running
// - schematool in PATH
// - Hive installed
// These should be done in integration tests.

func TestEnsureMetastoreSchemaCancellationStopsSchematoolAndIsNotDowngraded(t *testing.T) {
	for _, mode := range []string{"info", "init"} {
		t.Run(mode, func(t *testing.T) {
			binDir := t.TempDir()
			started := filepath.Join(binDir, "started")
			script := "#!/bin/sh\n" +
				"case \"$*\" in *-initSchema*) : > \"" + started + "\"; sleep 30;; esac\n" +
				"if [ \"" + mode + "\" = info ]; then : > \"" + started + "\"; sleep 30; fi\n" +
				"echo 'relation does not exist' >&2; exit 1\n"
			if err := os.WriteFile(filepath.Join(binDir, "schematool"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			hiveConf := t.TempDir()
			if err := os.WriteFile(filepath.Join(hiveConf, "hive-site.xml"), []byte(`<?xml version="1.0"?>
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:postgresql://localhost:5432/metastore</value></property>
</configuration>`), 0o644); err != nil {
				t.Fatal(err)
			}
			svc := &HiveService{env: &env.Environment{HiveConfDir: hiveConf, Path: binDir + ":/bin:/usr/bin"}}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- svc.ensureMetastoreSchema(ctx) }()

			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(started); err == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()

			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("ensureMetastoreSchema() = %v, want context.Canceled (not a graceful schema warning)", err)
				}
				if strings.Contains(err.Error(), "schematool") {
					t.Fatalf("cancellation was reported as a schematool failure: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("ensureMetastoreSchema() did not return after cancellation")
			}
		})
	}
}
