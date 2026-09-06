package env

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// DoctorSchemaVersion is the version of the machine-readable doctor report.
const DoctorSchemaVersion = 1

// DoctorCheck represents a single dependency check
type DoctorCheck struct {
	Command  string // Command name
	Required bool   // true if required, false if optional
	Found    bool   // true if command is available
}

// DoctorResult holds the results of all checks
type DoctorResult struct {
	Target      string        // Target context (e.g., "start hdfs")
	Checks      []DoctorCheck // All checks performed
	JavaMajor   int           // Java major version (0 if not found)
	HasFailures bool          // true if any required check failed
}

// DoctorDependencyReport contains the findings for one dependency class.
// Commands in Required and Optional retain the order used by the doctor policy.
type DoctorDependencyReport struct {
	Required []DoctorDependency `json:"required"`
	Optional []DoctorDependency `json:"optional"`
}

// DoctorDependency is a secret-free observation of one command lookup.
type DoctorDependency struct {
	Command string `json:"command"`
	Found   bool   `json:"found"`
}

// DoctorJavaReport records the detected Java major version and the version
// recommended by local-data.
type DoctorJavaReport struct {
	Major            int  `json:"major"`
	RecommendedMajor int  `json:"recommended_major"`
	IsRecommended    bool `json:"is_recommended"`
}

// DoctorOverallResult describes whether every required dependency was found.
type DoctorOverallResult struct {
	Healthy bool `json:"healthy"`
}

// DoctorJSONReport is the versioned, machine-readable doctor result.
type DoctorJSONReport struct {
	SchemaVersion int                    `json:"schema_version"`
	Target        string                 `json:"target"`
	Dependencies  DoctorDependencyReport `json:"dependencies"`
	Java          DoctorJavaReport       `json:"java"`
	Result        DoctorOverallResult    `json:"result"`
}

// RunDoctor performs dependency checking based on the target context
// Mirrors ld_doctor from doctor.sh
func RunDoctor(target string) *DoctorResult {
	var required, optional []string

	// Base requirements
	required = []string{"java"}
	optional = []string{"curl"}

	// Add context-specific requirements
	switch target {
	case "":
		// General check
		required = append(required, "brew")
		optional = append(optional, "spark-sql", "beeline")

	case "start hdfs":
		required = append(required, "hdfs")
		optional = append(optional, "jps")

	case "start yarn":
		required = append(required, "yarn")
		optional = append(optional, "jps")

	case "start hive":
		required = append(required, "hive")
		optional = append(optional, "beeline")

	case "init", "profile set", "profile list", "profile check", "profile diff":
		// These are handled by Go, no additional deps needed
		// In Bash version they check for cp/sed

	case "env exec", "env print":
		// Handled by Go, no additional deps
		// In Bash version they check for awk

	default:
		// Unknown target: baseline check
		required = append(required, "brew")
		optional = append(optional, "spark-sql", "beeline")
	}

	result := &DoctorResult{
		Target: target,
	}

	// Match the executable search order used by the runtime without applying a
	// profile overlay. Doctor must remain usable before profile initialization.
	commandEnv := doctorCommandEnvironment()

	// Check required commands
	for _, cmd := range required {
		found := doctorCommandFound(cmd, commandEnv)
		result.Checks = append(result.Checks, DoctorCheck{
			Command:  cmd,
			Required: true,
			Found:    found,
		})
		if !found {
			result.HasFailures = true
		}
	}

	// Check Java version
	javaDetector := NewJavaDetector()
	if doctorCommandFound("java", commandEnv) {
		result.JavaMajor = javaDetector.MajorVersion()
	}

	// Check optional commands
	for _, cmd := range optional {
		found := doctorCommandFound(cmd, commandEnv)
		result.Checks = append(result.Checks, DoctorCheck{
			Command:  cmd,
			Required: false,
			Found:    found,
		})
	}

	return result
}

// doctorCommandEnvironment builds the executable-search environment that a
// configured command would use, without requiring profile configuration. The
// selected installation directories come before the parent PATH so a stale or
// conflicting shell tool cannot make doctor disagree with command execution.
func doctorCommandEnvironment() []string {
	var selected []string

	if javaHome := NewJavaDetector().FindJavaHome(); javaHome != "" {
		selected = append(selected, filepath.Join(javaHome, "bin"))
	}
	if hadoop := FindHadoopInstall(); hadoop != nil {
		prefix := hadoop.Prefix
		if prefix == "" {
			prefix = hadoop.Home
		}
		if prefix != "" {
			selected = append(selected, filepath.Join(prefix, "bin"), filepath.Join(prefix, "sbin"))
		}
	}
	if hiveHome := FindHiveHome(); hiveHome != "" {
		selected = append(selected, filepath.Join(hiveHome, "bin"))
	}
	if sparkHome := FindSparkHome(); sparkHome != "" {
		selected = append(selected, filepath.Join(sparkHome, "bin"))
	}

	return []string{"PATH=" + util.DeduplicatePath(selected, os.Getenv("PATH"))}
}

func doctorCommandFound(command string, commandEnv []string) bool {
	_, err := ResolveExecutable(command, commandEnv)
	return err == nil
}

// Print prints the doctor check results
func (dr *DoctorResult) Print() error {
	return dr.PrintTo(os.Stdout)
}

// PrintTo renders the human-readable doctor report to w.
func (dr *DoctorResult) PrintTo(w io.Writer) error {
	targetStr := "general"
	if dr.Target != "" {
		targetStr = dr.Target
	}

	if _, err := fmt.Fprintf(w, "Doctor (%s):\n", targetStr); err != nil {
		return err
	}

	// Print check results
	for _, check := range dr.Checks {
		var status string
		msg := check.Command

		if check.Found {
			status = util.Colorf(util.Green, "OK  ")
		} else if check.Required {
			status = util.Colorf(util.BoldRed, "FAIL")
			msg = fmt.Sprintf("%s (required)", check.Command)
		} else {
			status = util.Colorf(util.Yellow, "WARN")
			msg = fmt.Sprintf("%s (optional)", check.Command)
		}

		if _, err := fmt.Fprintf(w, "  %s %s\n", status, msg); err != nil {
			return err
		}
	}

	// Java version warning
	if dr.JavaMajor != 0 && dr.JavaMajor != 17 {
		if _, err := fmt.Fprintf(w, "  %s java major version is %d (recommended: 17)\n", util.Colorf(util.Yellow, "WARN"), dr.JavaMajor); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "       Fix: install Java 17 and set JAVA_HOME"); err != nil {
			return err
		}
	}
	return nil
}

// JSONReport returns the stable, secret-free representation of this result.
func (dr *DoctorResult) JSONReport() DoctorJSONReport {
	target := dr.Target
	if target == "" {
		target = "general"
	}

	report := DoctorJSONReport{
		SchemaVersion: DoctorSchemaVersion,
		Target:        target,
		Java: DoctorJavaReport{
			Major:            dr.JavaMajor,
			RecommendedMajor: 17,
			IsRecommended:    dr.JavaMajor == 17,
		},
		Result: DoctorOverallResult{Healthy: !dr.HasFailures},
	}
	for _, check := range dr.Checks {
		dependency := DoctorDependency{Command: check.Command, Found: check.Found}
		if check.Required {
			report.Dependencies.Required = append(report.Dependencies.Required, dependency)
		} else {
			report.Dependencies.Optional = append(report.Dependencies.Optional, dependency)
		}
	}
	return report
}

// ExitCode returns the appropriate exit code
// 0 if all required checks passed, 1 if any failed
func (dr *DoctorResult) ExitCode() int {
	if dr.HasFailures {
		return 1
	}
	return 0
}
