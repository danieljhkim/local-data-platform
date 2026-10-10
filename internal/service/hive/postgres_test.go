package hive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsurePostgresJDBCDriverMissingJarRecommendsCurrentVersion(t *testing.T) {
	root := t.TempDir()
	hiveHome := filepath.Join(root, "hive")
	if err := os.MkdirAll(filepath.Join(hiveHome, "lib"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := EnsurePostgresJDBCDriver(hiveHome, "", "")
	if err == nil {
		t.Fatal("expected an error when no PostgreSQL JDBC jar is available")
	}

	const expectedURL = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar"
	if !strings.Contains(err.Error(), expectedURL) {
		t.Fatalf("missing-driver diagnostic does not recommend %q:\n%s", expectedURL, err)
	}
	if strings.Contains(err.Error(), "42.7.4") {
		t.Fatalf("missing-driver diagnostic still recommends the affected 42.7.4 release:\n%s", err)
	}
	assertOnlyFiles(t, root)
}

func TestEnsurePostgresJDBCDriverAcceptsUserProvidedJar(t *testing.T) {
	root := t.TempDir()
	hiveHome := filepath.Join(root, "hive")
	jarPath := filepath.Join(hiveHome, "lib", "postgresql-custom.jar")
	if err := os.MkdirAll(filepath.Dir(jarPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jarPath, []byte("test jar"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := EnsurePostgresJDBCDriver(hiveHome, "", root)
	if err != nil {
		t.Fatalf("EnsurePostgresJDBCDriver returned an error for a user-provided jar: %v", err)
	}
	if got != jarPath {
		t.Fatalf("EnsurePostgresJDBCDriver returned %q, want %q", got, jarPath)
	}
	assertOnlyFiles(t, root, jarPath)
}

func assertOnlyFiles(t *testing.T, root string, want ...string) {
	t.Helper()

	var got []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			got = append(got, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("found files %v, want only %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("found files %v, want only %v", got, want)
		}
	}
}
