package util

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

const (
	// HiveConnectionPasswordProperty is the Hadoop XML property that carries the Hive metastore password.
	HiveConnectionPasswordProperty             = "javax.jdo.option.ConnectionPassword"
	RedactedValue                              = "********"
	PublicFileMode                 os.FileMode = 0644
	PrivateFileMode                os.FileMode = 0600
)

// IsSensitivePropertyName reports whether a Hadoop XML property should be treated as secret.
func IsSensitivePropertyName(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), HiveConnectionPasswordProperty)
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
