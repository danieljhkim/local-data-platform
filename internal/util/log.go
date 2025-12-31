package util

import (
	"fmt"
	"os"
)

// Log prints an informational message to stderr
// Mirrors Bash ld_log function: echo "==> $*" >&2
func Log(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "==> %s\n", formatted)
}

// Die prints an error message to stderr and exits with status 1
// Mirrors Bash ld_die function
func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", formatted)
	os.Exit(1)
}

// Warn prints a warning message to stderr
func Warn(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "WARN: %s\n", formatted)
}
