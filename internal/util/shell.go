package util

import (
	"strings"
)

// DeduplicatePath deduplicates PATH components while preserving order
// Mirrors Bash PATH deduplication logic from ld_env_print
func DeduplicatePath(newParts []string, existingPath string) string {
	seen := make(map[string]bool)
	var result []string

	// Add new parts first (they have priority)
	for _, part := range newParts {
		part = strings.TrimSpace(part)
		if part != "" && !seen[part] {
			seen[part] = true
			result = append(result, part)
		}
	}

	// Add existing PATH components
	if existingPath != "" {
		for _, part := range strings.Split(existingPath, ":") {
			part = strings.TrimSpace(part)
			if part != "" && !seen[part] {
				seen[part] = true
				result = append(result, part)
			}
		}
	}

	return strings.Join(result, ":")
}

// ShellQuote quotes a string for safe use in shell commands
// Simple implementation: wraps in single quotes and escapes embedded single quotes
func ShellQuote(s string) string {
	// Replace single quotes with '\''
	escaped := strings.ReplaceAll(s, "'", "'\\''")
	return "'" + escaped + "'"
}

// ShellEscape escapes a string for use in export statements
// Similar to Bash printf %q
func ShellEscape(s string) string {
	// For simplicity, use ShellQuote for now
	// A more sophisticated version would check if quoting is needed
	if needsQuoting(s) {
		return ShellQuote(s)
	}
	return s
}

// needsQuoting checks if a string needs shell quoting
func needsQuoting(s string) bool {
	// Needs quoting if it contains spaces, special chars, or is empty
	if s == "" {
		return true
	}
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '"' || r == '\'' ||
			r == '$' || r == '\\' || r == '`' || r == '|' || r == '&' ||
			r == ';' || r == '(' || r == ')' || r == '<' || r == '>' {
			return true
		}
	}
	return false
}
