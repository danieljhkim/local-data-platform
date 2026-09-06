package env

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunDoctor_General(t *testing.T) {
	result := RunDoctor("")

	if result.Target != "" {
		t.Errorf("Target = %q, want empty string", result.Target)
	}

	// Should have some checks
	if len(result.Checks) == 0 {
		t.Error("No checks performed")
	}

	// Verify required commands are checked
	requiredFound := false
	for _, check := range result.Checks {
		if check.Command == "java" && check.Required {
			requiredFound = true
			break
		}
	}
	if !requiredFound {
		t.Error("Java required check not found")
	}
}

func TestRunDoctor_StartHDFS(t *testing.T) {
	result := RunDoctor("start hdfs")

	if result.Target != "start hdfs" {
		t.Errorf("Target = %q, want %q", result.Target, "start hdfs")
	}

	// Should check for hdfs command
	hdfsFound := false
	for _, check := range result.Checks {
		if check.Command == "hdfs" && check.Required {
			hdfsFound = true
			break
		}
	}
	if !hdfsFound {
		t.Error("HDFS required check not found")
	}

	// Should check for jps as optional
	jpsFound := false
	for _, check := range result.Checks {
		if check.Command == "jps" && !check.Required {
			jpsFound = true
			break
		}
	}
	if !jpsFound {
		t.Error("jps optional check not found")
	}
}

func TestRunDoctor_StartYARN(t *testing.T) {
	result := RunDoctor("start yarn")

	// Should check for yarn command
	yarnFound := false
	for _, check := range result.Checks {
		if check.Command == "yarn" && check.Required {
			yarnFound = true
			break
		}
	}
	if !yarnFound {
		t.Error("YARN required check not found")
	}
}

func TestRunDoctor_StartHive(t *testing.T) {
	result := RunDoctor("start hive")

	// Should check for hive command
	hiveFound := false
	for _, check := range result.Checks {
		if check.Command == "hive" && check.Required {
			hiveFound = true
			break
		}
	}
	if !hiveFound {
		t.Error("Hive required check not found")
	}

	// Should check for beeline as optional
	beelineFound := false
	for _, check := range result.Checks {
		if check.Command == "beeline" && !check.Required {
			beelineFound = true
			break
		}
	}
	if !beelineFound {
		t.Error("beeline optional check not found")
	}
}

func TestRunDoctorUsesSelectedInstallationPaths(t *testing.T) {
	parentBin := t.TempDir()
	hadoopHome := t.TempDir()
	hiveHome := t.TempDir()
	sparkHome := t.TempDir()
	for _, bin := range []string{
		filepath.Join(hadoopHome, "bin"),
		filepath.Join(hiveHome, "bin"),
		filepath.Join(sparkHome, "bin"),
	} {
		if err := os.MkdirAll(bin, 0755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", bin, err)
		}
	}

	for _, command := range []string{"hdfs", "hive", "beeline", "spark-sql"} {
		writeExecutable(t, parentBin, command, "#!/bin/sh\nexit 0\n")
	}
	writeExecutable(t, filepath.Join(hadoopHome, "bin"), "hdfs", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(hiveHome, "bin"), "hive", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(hiveHome, "bin"), "beeline", "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(sparkHome, "bin"), "spark-sql", "#!/bin/sh\nexit 0\n")

	t.Setenv("PATH", parentBin)
	t.Setenv("HADOOP_HOME", hadoopHome)
	t.Setenv("HIVE_HOME", hiveHome)
	t.Setenv("SPARK_HOME", sparkHome)

	for _, test := range []struct {
		target  string
		command string
		want    string
	}{
		{"start hdfs", "hdfs", filepath.Join(hadoopHome, "bin", "hdfs")},
		{"start hive", "hive", filepath.Join(hiveHome, "bin", "hive")},
		{"start hive", "beeline", filepath.Join(hiveHome, "bin", "beeline")},
		{"", "spark-sql", filepath.Join(sparkHome, "bin", "spark-sql")},
	} {
		t.Run(test.target+"/"+test.command, func(t *testing.T) {
			if path, err := ResolveExecutable(test.command, doctorCommandEnvironment()); err != nil || path != test.want {
				t.Fatalf("doctor resolution = %q, %v; want selected installation %q", path, err, test.want)
			}
			if !doctorCheckFound(RunDoctor(test.target), test.command) {
				t.Fatalf("doctor did not find selected %s", test.command)
			}
		})
	}
}

func TestRunDoctorRejectsMissingNonExecutableAndCurrentDirectoryTools(t *testing.T) {
	missingHome := filepath.Join(t.TempDir(), "missing")
	nonExecutableHome := t.TempDir()
	if err := os.MkdirAll(filepath.Join(nonExecutableHome, "bin"), 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(nonExecutableHome, "bin", "hive"), []byte("#!/bin/sh\n"), 0644); err != nil {
		t.Fatalf("write non-executable hive = %v", err)
	}

	t.Setenv("PATH", t.TempDir())
	t.Setenv("HADOOP_HOME", missingHome)
	t.Setenv("HIVE_HOME", missingHome)
	t.Setenv("SPARK_HOME", missingHome)
	if doctorCheckFound(RunDoctor("start hive"), "hive") {
		t.Fatal("doctor found a missing selected hive executable")
	}

	t.Setenv("HIVE_HOME", nonExecutableHome)
	if doctorCheckFound(RunDoctor("start hive"), "hive") {
		t.Fatal("doctor found a non-executable selected hive executable")
	}

	currentDir := t.TempDir()
	writeExecutable(t, currentDir, "hive", "#!/bin/sh\nexit 0\n")
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	if err := os.Chdir(currentDir); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
	t.Setenv("HIVE_HOME", missingHome)
	t.Setenv("PATH", ".")
	if doctorCheckFound(RunDoctor("start hive"), "hive") {
		t.Fatal("doctor accepted an implicit current-directory hive executable")
	}
}

func doctorCheckFound(result *DoctorResult, command string) bool {
	for _, check := range result.Checks {
		if check.Command == command {
			return check.Found
		}
	}
	return false
}

func TestRunDoctor_ProfileCommands(t *testing.T) {
	tests := []string{
		"init",
		"profile set",
		"profile list",
		"profile check",
		"profile diff",
		"env exec",
		"env print",
	}

	for _, target := range tests {
		t.Run(target, func(t *testing.T) {
			result := RunDoctor(target)

			if result.Target != target {
				t.Errorf("Target = %q, want %q", result.Target, target)
			}

			// These commands should only have baseline checks (java, curl)
			// No additional deps needed
			if len(result.Checks) == 0 {
				t.Error("No checks performed")
			}
		})
	}
}

func TestDoctorResult_ExitCode(t *testing.T) {
	tests := []struct {
		name         string
		hasFailures  bool
		expectedCode int
	}{
		{
			name:         "no failures",
			hasFailures:  false,
			expectedCode: 0,
		},
		{
			name:         "with failures",
			hasFailures:  true,
			expectedCode: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DoctorResult{
				HasFailures: tt.hasFailures,
			}

			code := result.ExitCode()
			if code != tt.expectedCode {
				t.Errorf("ExitCode() = %d, want %d", code, tt.expectedCode)
			}
		})
	}
}

func TestDoctorCheck_Structure(t *testing.T) {
	// Test that DoctorCheck can be created correctly
	check := DoctorCheck{
		Command:  "test-command",
		Required: true,
		Found:    false,
	}

	if check.Command != "test-command" {
		t.Errorf("Command = %q, want %q", check.Command, "test-command")
	}
	if !check.Required {
		t.Error("Expected Required to be true")
	}
	if check.Found {
		t.Error("Expected Found to be false")
	}
}

func TestDoctorResult_HasFailures(t *testing.T) {
	result := &DoctorResult{
		Target: "test",
		Checks: []DoctorCheck{
			{Command: "cmd1", Required: true, Found: true},
			{Command: "cmd2", Required: true, Found: false},
			{Command: "cmd3", Required: false, Found: false},
		},
		HasFailures: false,
	}

	// Check that we can detect failures
	hasRequiredFailure := false
	for _, check := range result.Checks {
		if check.Required && !check.Found {
			hasRequiredFailure = true
			break
		}
	}

	if !hasRequiredFailure {
		t.Error("Should have detected required command failure")
	}
}
