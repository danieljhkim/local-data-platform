package util

import (
	"fmt"
	"os"
	"sync"

	"golang.org/x/term"
)

// ANSI color codes
const (
	Reset    = "\033[0m"
	Bold     = "\033[1m"
	Dim      = "\033[2m"
	Red      = "\033[31m"
	Green    = "\033[32m"
	Yellow   = "\033[33m"
	Cyan     = "\033[36m"
	BoldRed  = "\033[1;31m"
	BoldCyan = "\033[1;36m"
)

// colorEnabled returns true if stderr is a TTY and NO_COLOR is not set.
var colorEnabled = sync.OnceValue(func() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	return term.IsTerminal(int(os.Stderr.Fd()))
})

// stdoutColorEnabled returns true if stdout is a TTY and NO_COLOR is not set.
var stdoutColorEnabled = sync.OnceValue(func() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	return term.IsTerminal(int(os.Stdout.Fd()))
})

// colorize wraps msg in ANSI color codes if color is enabled for stderr.
func colorize(c, msg string) string {
	if !colorEnabled() {
		return msg
	}
	return c + msg + Reset
}

// StdoutColorEnabled returns true if stdout is a TTY and NO_COLOR is not set.
func StdoutColorEnabled() bool {
	return stdoutColorEnabled()
}

// colorizeStdout wraps msg in ANSI color codes if color is enabled for stdout.
func colorizeStdout(c, msg string) string {
	if !stdoutColorEnabled() {
		return msg
	}
	return c + msg + Reset
}

// Log prints an informational message to stderr with a cyan bold "==>" prefix.
func Log(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", colorize(BoldCyan, "==>"), formatted)
}

// Success prints a success message to stderr with a green "==>" prefix.
func Success(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", colorize(Green, "==>"), colorize(Green, formatted))
}

// Section prints a bold section header to stdout (e.g., "==> hdfs").
func Section(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Println(colorizeStdout(Bold, "==> "+formatted))
}

// Die prints an error message to stderr and exits with status 1.
func Die(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", colorize(BoldRed, "ERROR:"), colorize(BoldRed, formatted))
	os.Exit(1)
}

// Warn prints a warning message to stderr.
func Warn(msg string, args ...interface{}) {
	formatted := fmt.Sprintf(msg, args...)
	fmt.Fprintf(os.Stderr, "%s %s\n", colorize(Yellow, "WARN:"), colorize(Yellow, formatted))
}

// Colorf formats a string with the given color for stdout output.
func Colorf(c, format string, args ...interface{}) string {
	formatted := fmt.Sprintf(format, args...)
	return colorizeStdout(c, formatted)
}

// StatusLine prints a colored status line to stdout.
// Green for running, red for stopped.
func StatusLine(name string, running bool, pid int) {
	if running {
		fmt.Println(colorizeStdout(Green, fmt.Sprintf("%s: running (pid %d)", name, pid)))
	} else {
		fmt.Println(colorizeStdout(Red, fmt.Sprintf("%s: stopped", name)))
	}
}

// StatusTableRow represents a single row in a status table.
type StatusTableRow struct {
	Name   string
	Status string // Display text for status column
	Detail string // Extra info (PID, port, cmd)
	Ok     bool   // true = green, false = red
}

// StatusTable prints rows as an aligned, colored table.
func StatusTable(rows []StatusTableRow) {
	if len(rows) == 0 {
		return
	}

	// Compute column widths (using raw text length, not ANSI-colored length)
	nameW, statusW := 0, 0
	for _, r := range rows {
		if len(r.Name) > nameW {
			nameW = len(r.Name)
		}
		if len(r.Status) > statusW {
			statusW = len(r.Status)
		}
	}

	for _, r := range rows {
		c := Green
		if !r.Ok {
			c = Red
		}
		status := colorizeStdout(c, r.Status)
		detail := ""
		if r.Detail != "" {
			detail = colorizeStdout(Dim, r.Detail)
		}
		fmt.Printf("  %-*s  %s  %s\n", nameW, r.Name, status, detail)
	}
}
