package util

import (
	"testing"
)

func TestDeduplicatePath(t *testing.T) {
	tests := []struct {
		name         string
		newParts     []string
		existingPath string
		expected     string
	}{
		{
			name:         "no duplicates",
			newParts:     []string{"/usr/local/bin", "/usr/bin"},
			existingPath: "/bin:/sbin",
			expected:     "/usr/local/bin:/usr/bin:/bin:/sbin",
		},
		{
			name:         "with duplicates",
			newParts:     []string{"/usr/local/bin", "/usr/bin"},
			existingPath: "/usr/bin:/bin:/usr/local/bin",
			expected:     "/usr/local/bin:/usr/bin:/bin",
		},
		{
			name:         "empty existing",
			newParts:     []string{"/usr/local/bin", "/usr/bin"},
			existingPath: "",
			expected:     "/usr/local/bin:/usr/bin",
		},
		{
			name:         "empty new parts",
			newParts:     []string{},
			existingPath: "/usr/bin:/bin",
			expected:     "/usr/bin:/bin",
		},
		{
			name:         "all duplicates",
			newParts:     []string{"/usr/bin", "/bin"},
			existingPath: "/usr/bin:/bin",
			expected:     "/usr/bin:/bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DeduplicatePath(tt.newParts, tt.existingPath)
			if result != tt.expected {
				t.Errorf("DeduplicatePath() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "hello",
			expected: "'hello'",
		},
		{
			name:     "string with space",
			input:    "hello world",
			expected: "'hello world'",
		},
		{
			name:     "string with single quote",
			input:    "it's",
			expected: "'it'\\''s'",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
		{
			name:     "path with spaces",
			input:    "/path/to/my documents",
			expected: "'/path/to/my documents'",
		},
		{
			name:     "already quoted",
			input:    "'hello'",
			expected: "''\\''hello'\\'''",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShellQuote(tt.input)
			if result != tt.expected {
				t.Errorf("ShellQuote(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
