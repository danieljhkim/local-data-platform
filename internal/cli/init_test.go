package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/danieljhkim/local-data-platform/internal/util"
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

func TestInit_DBPasswordConfirmationRedactsAndAllowsKeepOrReplace(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantPassword string
	}{
		{
			name:         "enter keeps current password",
			input:        "\n\n\n\n",
			wantPassword: "existing-secret",
		},
		{
			name:         "typed value replaces current password",
			input:        "\n\n\nreplacement-secret\n",
			wantPassword: "replacement-secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir := t.TempDir()
			paths := config.NewPaths("", baseDir)
			sm := config.NewSettingsManager(paths)
			if err := sm.Save(&config.Settings{
				User:       "daniel",
				DBType:     "postgres",
				DBURL:      "jdbc:postgresql://localhost:5432/metastore",
				DBPassword: "existing-secret",
			}); err != nil {
				t.Fatalf("save settings: %v", err)
			}

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
			cmd.SetIn(strings.NewReader(tt.input))

			if err := cmd.Execute(); err != nil {
				t.Fatalf("init returned error: %v", err)
			}

			output := out.String()
			if strings.Contains(output, "existing-secret") || strings.Contains(output, "replacement-secret") {
				t.Fatalf("db-password confirmation leaked a password:\n%s", output)
			}
			if !strings.Contains(output, "confirm db-password to be: ********") {
				t.Fatalf("missing redacted db-password confirmation:\n%s", output)
			}

			settings, err := sm.Load()
			if err != nil {
				t.Fatalf("load settings: %v", err)
			}
			if settings.DBPassword != tt.wantPassword {
				t.Fatalf("DBPassword = %q, want %q", settings.DBPassword, tt.wantPassword)
			}
		})
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

func TestInit_PasswordBearingURLIsRedacted(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	sm := config.NewSettingsManager(paths)
	if err := sm.Save(&config.Settings{
		User:       "daniel",
		DBType:     "postgres",
		DBURL:      "jdbc:postgresql://alice:" + secret + "@localhost:5432/metastore",
		DBPassword: secret,
	}); err != nil {
		t.Fatalf("save settings: %v", err)
	}

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

	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("init leaked secret:\n%s", combined)
	}
	if !strings.Contains(out.String(), "confirm db-url to be: jdbc:postgresql://alice:********@localhost:5432/metastore") {
		t.Fatalf("expected redacted db-url confirmation:\n%s", out.String())
	}
}

func TestInit_DBPasswordFileDoesNotPlaceSecretOnArgv(t *testing.T) {
	const secret = "s3cret-value"
	baseDir := t.TempDir()
	paths := config.NewPaths("", baseDir)
	pwFile := filepath.Join(baseDir, "pw")
	if err := os.WriteFile(pwFile, []byte(secret+"\n"), 0600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

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
	cmd.SetIn(strings.NewReader("\npostgres\njdbc:postgresql://localhost:5432/metastore\n\n"))
	cmd.SetArgs([]string{"--db-password-file", pwFile})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}

	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("init leaked secret:\n%s", combined)
	}

	settings, err := smLoad(t, paths)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestInit_DeprecatedDBPasswordFlagWarns(t *testing.T) {
	const secret = "s3cret-value"
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
	cmd.SetIn(strings.NewReader("\npostgres\njdbc:postgresql://localhost:5432/metastore\n\n"))
	cmd.SetArgs([]string{"--db-password", secret})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}
	if !strings.Contains(errBuf.String(), "deprecated") && !strings.Contains(errBuf.String(), "Deprecated") {
		t.Fatalf("expected deprecation warning, got: %s", errBuf.String())
	}
	if strings.Contains(out.String(), secret) {
		t.Fatalf("stdout leaked secret:\n%s", out.String())
	}

	settings, err := smLoad(t, paths)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func TestInit_LocalDataDBPasswordEnv(t *testing.T) {
	const secret = "s3cret-value"
	t.Setenv(util.DBPasswordEnvVar, secret)

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
	cmd.SetIn(strings.NewReader("\npostgres\njdbc:postgresql://localhost:5432/metastore\n\n"))

	if err := cmd.Execute(); err != nil {
		t.Fatalf("init returned error: %v", err)
	}
	combined := out.String() + errBuf.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("init leaked secret:\n%s", combined)
	}

	settings, err := smLoad(t, paths)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	if settings.DBPassword != secret {
		t.Fatalf("DBPassword = %q", settings.DBPassword)
	}
}

func smLoad(t *testing.T, paths *config.Paths) (*config.Settings, error) {
	t.Helper()
	return config.NewSettingsManager(paths).Load()
}
