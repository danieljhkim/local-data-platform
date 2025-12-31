package env

import (
	"strings"
	"testing"
)

func TestEnvironment_Export(t *testing.T) {
	env := &Environment{
		BaseDir:       "/test/base",
		RepoRoot:      "/test/repo",
		ActiveProfile: "hdfs",
		HadoopHome:    "/usr/local/hadoop",
		HiveHome:      "/usr/local/hive",
		SparkHome:     "/usr/local/spark",
		JavaHome:      "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home",
		Path:          "/usr/local/bin:/usr/bin:/bin",
	}

	exported := env.Export()

	// Verify it returns a slice of strings
	if len(exported) == 0 {
		t.Error("Export() returned empty slice")
	}

	// Verify key environment variables are present
	expectedVars := map[string]string{
		"BASE_DIR":    "/test/base",
		"HADOOP_HOME": "/usr/local/hadoop",
		"HIVE_HOME":   "/usr/local/hive",
		"SPARK_HOME":  "/usr/local/spark",
		"JAVA_HOME":   "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home",
		"PATH":        "/usr/local/bin:/usr/bin:/bin",
	}

	exportedMap := make(map[string]string)
	for _, line := range exported {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			exportedMap[parts[0]] = parts[1]
		}
	}

	for key, expectedValue := range expectedVars {
		if actualValue, ok := exportedMap[key]; !ok {
			t.Errorf("Environment variable %s not found in exported vars", key)
		} else if actualValue != expectedValue {
			t.Errorf("Environment variable %s = %q, want %q", key, actualValue, expectedValue)
		}
	}
}

func TestEnvironment_Export_PathLast(t *testing.T) {
	env := &Environment{
		Path:       "/custom/bin:/usr/bin",
		HadoopHome: "/hadoop",
	}

	exported := env.Export()

	// PATH should be last in the exported list
	if len(exported) == 0 {
		t.Fatal("Export() returned empty slice")
	}

	lastVar := exported[len(exported)-1]
	if !strings.HasPrefix(lastVar, "PATH=") {
		t.Errorf("Last exported var = %q, want PATH=...", lastVar)
	}
}

func TestEnvironment_Export_EmptyValues(t *testing.T) {
	env := &Environment{
		BaseDir:    "/base",
		HadoopHome: "", // Empty value should not be exported
		Path:       "/usr/bin",
	}

	exported := env.Export()

	exportedMap := make(map[string]string)
	for _, line := range exported {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			exportedMap[parts[0]] = parts[1]
		}
	}

	// Empty HADOOP_HOME should not be in exports
	if _, ok := exportedMap["HADOOP_HOME"]; ok {
		t.Error("Empty HADOOP_HOME should not be exported")
	}

	// Non-empty values should be present
	if exportedMap["BASE_DIR"] != "/base" {
		t.Errorf("BASE_DIR not exported correctly")
	}
}
