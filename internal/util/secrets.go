package util

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const (
	// HiveConnectionPasswordProperty is the Hadoop XML property that carries the Hive metastore password.
	HiveConnectionPasswordProperty             = "javax.jdo.option.ConnectionPassword"
	RedactedValue                              = "********"
	PublicFileMode                 os.FileMode = 0644
	PrivateFileMode                os.FileMode = 0600
	// DBPasswordEnvVar is the secret-safe environment variable for the Hive metastore password.
	DBPasswordEnvVar = "LOCAL_DATA_DB_PASSWORD"
	// DeprecatedPasswordArgWarning is emitted when a password is supplied on argv.
	DeprecatedPasswordArgWarning = "WARNING: passing db-password on the command line is deprecated; it exposes the secret in process listings and shell history. Use a prompt, --stdin, --from-file, --db-password-file, or LOCAL_DATA_DB_PASSWORD."
)

var (
	jdbcUserinfoPasswordPattern = regexp.MustCompile(`(://[^:/?#@]*):([^@/]+)@`)
	jdbcQueryPasswordPattern    = regexp.MustCompile(`(?i)([?&;](?:password|passwd|pwd)=)([^&;]*)`)
)

// IsSensitivePropertyName reports whether a Hadoop XML property should be treated as secret.
func IsSensitivePropertyName(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), HiveConnectionPasswordProperty)
}

// RedactJDBCURL masks password userinfo and password query parameters in JDBC (or JDBC-like) URLs.
func RedactJDBCURL(raw string) string {
	if raw == "" {
		return raw
	}
	s := jdbcUserinfoPasswordPattern.ReplaceAllString(raw, "${1}:"+RedactedValue+"@")
	return jdbcQueryPasswordPattern.ReplaceAllString(s, "${1}"+RedactedValue)
}

// RedactSensitiveHadoopXML masks secret values in Hadoop-style XML while leaving non-secret properties visible.
func RedactSensitiveHadoopXML(data []byte) ([]byte, bool, error) {
	if !bytes.Contains(data, []byte(HiveConnectionPasswordProperty)) {
		return data, false, nil
	}

	var config HadoopConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		return nil, false, fmt.Errorf("failed to parse Hadoop XML for redaction: %w", err)
	}

	if !config.redactSensitiveValues() {
		return data, false, nil
	}

	redacted, err := xml.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, false, fmt.Errorf("failed to marshal redacted Hadoop XML: %w", err)
	}

	return []byte(xml.Header + string(redacted) + "\n"), true, nil
}

func (c *HadoopConfiguration) redactSensitiveValues() bool {
	found := false
	for i, prop := range c.Properties {
		if IsSensitivePropertyName(prop.Name) {
			c.Properties[i].Value = RedactedValue
			found = true
		}
	}
	return found
}

func (c *HadoopConfiguration) containsSensitiveProperty() bool {
	if c == nil {
		return false
	}
	for _, prop := range c.Properties {
		if IsSensitivePropertyName(prop.Name) {
			return true
		}
	}
	return false
}

func (c *HadoopConfiguration) fileMode() os.FileMode {
	if c.containsSensitiveProperty() {
		return PrivateFileMode
	}
	return PublicFileMode
}

func fileModeForContent(data []byte, fallback os.FileMode) os.FileMode {
	if bytes.Contains(data, []byte(HiveConnectionPasswordProperty)) {
		return PrivateFileMode
	}
	if fallback == 0 {
		return PublicFileMode
	}
	return fallback
}
