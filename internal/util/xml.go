package util

import (
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

// HadoopConfiguration represents a Hadoop XML configuration file
// Mirrors the structure of core-site.xml, hdfs-site.xml, etc.
type HadoopConfiguration struct {
	XMLName    xml.Name         `xml:"configuration"`
	Properties []HadoopProperty `xml:"property"`
}

// HadoopProperty represents a single property in Hadoop XML config
type HadoopProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

// ParseHadoopXML parses a Hadoop XML configuration file
func ParseHadoopXML(path string) (*HadoopConfiguration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read XML file: %w", err)
	}

	var config HadoopConfiguration
	if err := xml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse XML: %w", err)
	}

	return &config, nil
}

// GetProperty returns the value of a property by name
// Returns empty string if property not found
func (c *HadoopConfiguration) GetProperty(name string) string {
	for _, prop := range c.Properties {
		if prop.Name == name {
			return prop.Value
		}
	}
	return ""
}

// SetProperty sets or updates a property value
func (c *HadoopConfiguration) SetProperty(name, value string) {
	for i, prop := range c.Properties {
		if prop.Name == name {
			c.Properties[i].Value = value
			return
		}
	}
	// Property not found, add it
	c.Properties = append(c.Properties, HadoopProperty{
		Name:  name,
		Value: value,
	})
}

// WriteXML writes the configuration back to a file
func (c *HadoopConfiguration) WriteXML(path string) error {
	data, err := xml.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal XML: %w", err)
	}

	// Add XML header
	xmlData := []byte(xml.Header + string(data) + "\n")

	if err := os.WriteFile(path, xmlData, 0644); err != nil {
		return fmt.Errorf("failed to write XML file: %w", err)
	}

	return nil
}

// ParseFileURIs parses a comma-separated list of file:// URIs
// Returns the local filesystem paths
// Example: "file:///data/hdfs,file:///backup/hdfs" -> ["/data/hdfs", "/backup/hdfs"]
func ParseFileURIs(value string) []string {
	var paths []string

	for _, uri := range strings.Split(value, ",") {
		uri = strings.TrimSpace(uri)

		// Handle file:// URIs
		if strings.HasPrefix(uri, "file:") {
			// Remove "file:" prefix
			path := strings.TrimPrefix(uri, "file:")
			// Remove leading slashes (file:// or file:///)
			path = strings.TrimLeft(path, "/")
			// Re-add single leading slash for absolute path
			if path != "" {
				paths = append(paths, "/"+path)
			}
		} else if uri != "" {
			// Non-URI path, use as-is
			paths = append(paths, uri)
		}
	}

	return paths
}

// ParseNameNodeDirs parses the dfs.namenode.name.dir property
// Returns a list of local filesystem paths where NameNode data is stored
func ParseNameNodeDirs(confPath string) ([]string, error) {
	config, err := ParseHadoopXML(confPath)
	if err != nil {
		return nil, err
	}

	value := config.GetProperty("dfs.namenode.name.dir")
	if value == "" {
		return nil, fmt.Errorf("dfs.namenode.name.dir not found in %s", confPath)
	}

	paths := ParseFileURIs(value)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no valid paths found in dfs.namenode.name.dir")
	}

	return paths, nil
}
