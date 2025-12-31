package env

import (
	"fmt"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

// DoctorCheck represents a single dependency check
type DoctorCheck struct {
	Command  string // Command name
	Required bool   // true if required, false if optional
	Found    bool   // true if command is available
}

// DoctorResult holds the results of all checks
type DoctorResult struct {
	Target      string         // Target context (e.g., "start hdfs")
	Checks      []DoctorCheck  // All checks performed
	JavaMajor   int            // Java major version (0 if not found)
	HasFailures bool           // true if any required check failed
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

	case "profile init", "profile set", "profile list", "profile check":
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

	// Check required commands
	detector := NewToolDetector()
	for _, cmd := range required {
		found := detector.IsInstalled(cmd)
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
	if javaDetector.IsInstalled() {
		result.JavaMajor = javaDetector.MajorVersion()
	}

	// Check optional commands
	for _, cmd := range optional {
		found := detector.IsInstalled(cmd)
		result.Checks = append(result.Checks, DoctorCheck{
			Command:  cmd,
			Required: false,
			Found:    found,
		})
	}

	return result
}

// Print prints the doctor check results
func (dr *DoctorResult) Print() {
	targetStr := "general"
	if dr.Target != "" {
		targetStr = dr.Target
	}

	util.Log("Doctor (%s):", targetStr)

	// Print check results
	for _, check := range dr.Checks {
		status := "OK  "
		msg := check.Command

		if !check.Found {
			if check.Required {
				status = "FAIL"
				msg = fmt.Sprintf("%s (required)", check.Command)
			} else {
				status = "WARN"
				msg = fmt.Sprintf("%s (optional)", check.Command)
			}
		}

		fmt.Printf("  %s %s\n", status, msg)
	}

	// Java version warning
	if dr.JavaMajor != 0 && dr.JavaMajor != 17 {
		fmt.Printf("  WARN java major version is %d (recommended: 17)\n", dr.JavaMajor)
		fmt.Printf("       Fix: install Java 17 and set JAVA_HOME\n")
	}
}

// ExitCode returns the appropriate exit code
// 0 if all required checks passed, 1 if any failed
func (dr *DoctorResult) ExitCode() int {
	if dr.HasFailures {
		return 1
	}
	return 0
}
