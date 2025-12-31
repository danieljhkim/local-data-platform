package util

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestParseHadoopXML(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name         string
		xmlContent   string
		expectError  bool
		validateFunc func(t *testing.T, config *HadoopConfiguration)
	}{
		{
			name: "valid XML with single property",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>test.property</name>
    <value>test value</value>
  </property>
</configuration>`,
			expectError: false,
			validateFunc: func(t *testing.T, config *HadoopConfiguration) {
				if len(config.Properties) != 1 {
					t.Errorf("Expected 1 property, got %d", len(config.Properties))
				}
				if config.Properties[0].Name != "test.property" {
					t.Errorf("Name = %q, want %q", config.Properties[0].Name, "test.property")
				}
				if config.Properties[0].Value != "test value" {
					t.Errorf("Value = %q, want %q", config.Properties[0].Value, "test value")
				}
			},
		},
		{
			name: "valid XML with multiple properties",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>prop1</name>
    <value>value1</value>
  </property>
  <property>
    <name>prop2</name>
    <value>value2</value>
  </property>
  <property>
    <name>prop3</name>
    <value>value3</value>
  </property>
</configuration>`,
			expectError: false,
			validateFunc: func(t *testing.T, config *HadoopConfiguration) {
				if len(config.Properties) != 3 {
					t.Errorf("Expected 3 properties, got %d", len(config.Properties))
				}
			},
		},
		{
			name: "malformed XML",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>test</name>
    <value>unclosed`,
			expectError: true,
			validateFunc: func(t *testing.T, config *HadoopConfiguration) {
				// No validation for error case
			},
		},
		{
			name: "empty XML file",
			xmlContent: `<?xml version="1.0"?>
<configuration>
</configuration>`,
			expectError: false,
			validateFunc: func(t *testing.T, config *HadoopConfiguration) {
				if len(config.Properties) != 0 {
					t.Errorf("Expected 0 properties, got %d", len(config.Properties))
				}
			},
		},
		{
			name: "XML with missing name tag",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <value>value only</value>
  </property>
</configuration>`,
			expectError: false,
			validateFunc: func(t *testing.T, config *HadoopConfiguration) {
				if len(config.Properties) != 1 {
					t.Errorf("Expected 1 property, got %d", len(config.Properties))
				}
				if config.Properties[0].Name != "" {
					t.Errorf("Name should be empty, got %q", config.Properties[0].Name)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write XML to temp file
			xmlFile := filepath.Join(tmpDir, tt.name+".xml")
			err := os.WriteFile(xmlFile, []byte(tt.xmlContent), 0644)
			if err != nil {
				t.Fatalf("Failed to write test XML: %v", err)
			}

			// Parse XML
			config, err := ParseHadoopXML(xmlFile)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				tt.validateFunc(t, config)
			}
		})
	}
}

func TestGetProperty(t *testing.T) {
	tmpDir := t.TempDir()

	xmlContent := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>prop1</name>
    <value>value1</value>
  </property>
  <property>
    <name>prop2</name>
    <value>value2</value>
  </property>
  <property>
    <name>duplicate</name>
    <value>first</value>
  </property>
  <property>
    <name>duplicate</name>
    <value>second</value>
  </property>
</configuration>`

	xmlFile := filepath.Join(tmpDir, "test.xml")
	os.WriteFile(xmlFile, []byte(xmlContent), 0644)

	config, err := ParseHadoopXML(xmlFile)
	if err != nil {
		t.Fatalf("Failed to parse XML: %v", err)
	}

	tests := []struct {
		name          string
		propertyName  string
		expectedValue string
	}{
		{
			name:          "property exists",
			propertyName:  "prop1",
			expectedValue: "value1",
		},
		{
			name:          "property not found",
			propertyName:  "nonexistent",
			expectedValue: "",
		},
		{
			name:          "duplicate property (first wins)",
			propertyName:  "duplicate",
			expectedValue: "first",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := config.GetProperty(tt.propertyName)

			if value != tt.expectedValue {
				t.Errorf("value = %q, want %q", value, tt.expectedValue)
			}
		})
	}
}

func TestParseNameNodeDirs(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name         string
		xmlContent   string
		expectedDirs []string
		expectError  bool
	}{
		{
			name: "single file:// URI",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///tmp/hadoop/namenode</value>
  </property>
</configuration>`,
			expectedDirs: []string{"/tmp/hadoop/namenode"},
			expectError:  false,
		},
		{
			name: "multiple comma-separated URIs",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///tmp/nn1,file:///tmp/nn2,file:///tmp/nn3</value>
  </property>
</configuration>`,
			expectedDirs: []string{"/tmp/nn1", "/tmp/nn2", "/tmp/nn3"},
			expectError:  false,
		},
		{
			name: "URIs without file:// prefix",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/tmp/nn1,/tmp/nn2</value>
  </property>
</configuration>`,
			expectedDirs: []string{"/tmp/nn1", "/tmp/nn2"},
			expectError:  false,
		},
		{
			name: "mixed URIs with and without file:// prefix",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///tmp/nn1,/tmp/nn2</value>
  </property>
</configuration>`,
			expectedDirs: []string{"/tmp/nn1", "/tmp/nn2"},
			expectError:  false,
		},
		{
			name: "empty dfs.namenode.name.dir value",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value></value>
  </property>
</configuration>`,
			expectedDirs: nil,
			expectError:  true,
		},
		{
			name: "missing dfs.namenode.name.dir property",
			xmlContent: `<?xml version="1.0"?>
<configuration>
  <property>
    <name>other.property</name>
    <value>value</value>
  </property>
</configuration>`,
			expectedDirs: nil,
			expectError:  true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write XML to temp file (use index to avoid spaces/slashes in directory name)
			confDir := filepath.Join(tmpDir, fmt.Sprintf("test%d", i))
			os.MkdirAll(confDir, 0755)
			xmlFile := filepath.Join(confDir, "hdfs-site.xml")
			err := os.WriteFile(xmlFile, []byte(tt.xmlContent), 0644)
			if err != nil {
				t.Fatalf("Failed to write test XML: %v", err)
			}

			// Parse NameNode dirs (pass the XML file path, not the directory)
			dirs, err := ParseNameNodeDirs(xmlFile)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if len(dirs) != len(tt.expectedDirs) {
					t.Errorf("Got %d dirs, want %d", len(dirs), len(tt.expectedDirs))
				}
				for i, dir := range dirs {
					if dir != tt.expectedDirs[i] {
						t.Errorf("Dir[%d] = %q, want %q", i, dir, tt.expectedDirs[i])
					}
				}
			}
		})
	}
}
