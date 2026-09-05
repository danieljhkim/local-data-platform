package env

import (
	"bytes"
	"encoding/json"
	"testing"

	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
)

func TestDoctorJSONWritesOneDocumentAndPreservesExitSemantics(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		result      *envpkg.DoctorResult
		wantErr     bool
		wantHealthy bool
	}{
		{
			name:        "healthy",
			args:        []string{"--json"},
			result:      &envpkg.DoctorResult{Checks: []envpkg.DoctorCheck{{Command: "java", Required: true, Found: true}, {Command: "brew", Required: true, Found: true}, {Command: "curl", Found: true}}, JavaMajor: 17},
			wantHealthy: true,
		},
		{
			name:    "required missing",
			args:    []string{"start", "hdfs", "--json"},
			result:  &envpkg.DoctorResult{Target: "start hdfs", Checks: []envpkg.DoctorCheck{{Command: "java", Required: true, Found: true}, {Command: "hdfs", Required: true, Found: false}, {Command: "jps", Found: true}}, JavaMajor: 17, HasFailures: true},
			wantErr: true,
		},
		{
			name:        "optional missing",
			args:        []string{"--json"},
			result:      &envpkg.DoctorResult{Checks: []envpkg.DoctorCheck{{Command: "java", Required: true, Found: true}, {Command: "brew", Required: true, Found: true}, {Command: "spark-sql", Found: false}}, JavaMajor: 17},
			wantHealthy: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newDoctorCmdWithRunner(nil, func(string) *envpkg.DoctorResult { return tt.result })
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Execute() error = %v, want error=%t", err, tt.wantErr)
			}
			var report envpkg.DoctorJSONReport
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
				t.Fatalf("stdout is not one JSON document: %v; output=%q", err, stdout.String())
			}
			if report.SchemaVersion != envpkg.DoctorSchemaVersion || report.Result.Healthy != tt.wantHealthy {
				t.Fatalf("report = %#v", report)
			}
			if stderr.Len() != 0 {
				t.Fatalf("JSON command wrote diagnostics to stderr: %q", stderr.String())
			}
			if stdout.String()[len(stdout.String())-1] != '\n' {
				t.Fatalf("JSON output is not newline terminated: %q", stdout.String())
			}
		})
	}
}

func TestDoctorHumanOutputUsesCommandWriter(t *testing.T) {
	result := &envpkg.DoctorResult{Checks: []envpkg.DoctorCheck{{Command: "java", Required: true, Found: true}}}
	cmd := newDoctorCmdWithRunner(nil, func(string) *envpkg.DoctorResult { return result })
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); got == "" || bytes.Contains(stdout.Bytes(), []byte("schema_version")) {
		t.Fatalf("human output = %q", got)
	}
}
