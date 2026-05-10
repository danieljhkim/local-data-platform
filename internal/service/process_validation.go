package service

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// PIDValidator checks that a PID still belongs to the expected service before
// ProcessManager sends a signal based on a PID file.
type PIDValidator func(name string, pid int) error

// ProcessInspector returns command-line and environment details for a PID.
type ProcessInspector func(pid int) (string, error)

// InspectProcess returns process details from ps. On macOS, ps eww includes
// environment variables; the fallback still gives us command-line ownership.
func InspectProcess(pid int) (string, error) {
	pidStr := strconv.Itoa(pid)

	output, err := exec.Command("ps", "eww", "-p", pidStr).Output()
	if err == nil && strings.TrimSpace(string(output)) != "" {
		return string(output), nil
	}

	output, fallbackErr := exec.Command("ps", "-p", pidStr, "-o", "command=").Output()
	if fallbackErr == nil && strings.TrimSpace(string(output)) != "" {
		return string(output), nil
	}
	if err != nil {
		return "", err
	}
	return "", fallbackErr
}

// NewProcessMatchValidator builds a PID ownership validator from per-service
// command-line/environment substrings.
func NewProcessMatchValidator(label string, patterns map[string][]string) PIDValidator {
	return NewProcessMatchValidatorWithInspector(label, patterns, InspectProcess)
}

// NewProcessMatchValidatorWithInspector is the testable form of
// NewProcessMatchValidator.
func NewProcessMatchValidatorWithInspector(label string, patterns map[string][]string, inspector ProcessInspector) PIDValidator {
	return func(name string, pid int) error {
		expected := patterns[name]
		if len(expected) == 0 {
			return fmt.Errorf("refusing to stop %s %s pid %d: no ownership patterns configured", label, name, pid)
		}

		details, err := inspector(pid)
		if err != nil {
			return fmt.Errorf("refusing to stop %s %s pid %d: could not inspect process: %w", label, name, pid, err)
		}

		for _, pattern := range expected {
			if strings.Contains(details, pattern) {
				return nil
			}
		}

		return fmt.Errorf("refusing to stop %s %s pid %d: process command line/environment does not match expected service", label, name, pid)
	}
}
