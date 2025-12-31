package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/util"
)

func TestProfileManager_Init(t *testing.T) {
	tests := []struct {
		name        string
		force       bool
		preExist    bool
		expectError bool
	}{
		{
			name:        "initialize fresh",
			force:       false,
			preExist:    false,
			expectError: false,
		},
		{
			name:        "already initialized without force",
			force:       false,
			preExist:    true,
			expectError: false, // No error, but should not overwrite
		},
		{
			name:        "already initialized with force",
			force:       true,
			preExist:    true,
			expectError: false, // Should overwrite
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directories
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			// Create repo profiles directory with test profiles
			repoProfiles := filepath.Join(repoRoot, "conf", "profiles")
			localProfile := filepath.Join(repoProfiles, "local")
			hdfsProfile := filepath.Join(repoProfiles, "hdfs")

			util.MkdirAll(filepath.Join(localProfile, "hive"))
			util.MkdirAll(filepath.Join(localProfile, "spark"))
			util.MkdirAll(filepath.Join(hdfsProfile, "hadoop"))
			util.MkdirAll(filepath.Join(hdfsProfile, "hive"))
			util.MkdirAll(filepath.Join(hdfsProfile, "spark"))

			// Create dummy config files
			os.WriteFile(filepath.Join(localProfile, "hive", "hive-site.xml"), []byte("<configuration></configuration>"), 0644)
			os.WriteFile(filepath.Join(hdfsProfile, "hive", "hive-site.xml"), []byte("<configuration></configuration>"), 0644)

			// Pre-create user profiles if needed
			userProfiles := filepath.Join(baseDir, "conf", "profiles")
			if tt.preExist {
				util.MkdirAll(userProfiles)
				// Create a marker file to verify overwrite behavior
				os.WriteFile(filepath.Join(userProfiles, "marker.txt"), []byte("old"), 0644)
			}

			// Create paths and profile manager
			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			// Run init
			err := pm.Init(tt.force)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// For non-force with pre-existing, Init should return early and not overwrite
			if tt.preExist && !tt.force {
				// Verify marker was NOT removed (profiles not overwritten)
				if !util.FileExists(filepath.Join(userProfiles, "marker.txt")) {
					t.Error("Marker file removed without --force (should be preserved)")
				}
				return
			}

			// Otherwise, verify user profiles directory exists and profiles were copied
			if !util.DirExists(userProfiles) {
				t.Error("User profiles directory not created")
			}

			// Verify profiles were copied
			if !util.DirExists(filepath.Join(userProfiles, "local")) {
				t.Error("Local profile not copied")
			}
			if !util.DirExists(filepath.Join(userProfiles, "hdfs")) {
				t.Error("HDFS profile not copied")
			}

			// For force overwrite, verify marker was removed
			if tt.preExist && tt.force {
				if util.FileExists(filepath.Join(userProfiles, "marker.txt")) {
					t.Error("Marker file not removed with --force")
				}
			}
		})
	}
}

func TestProfileManager_List(t *testing.T) {
	tests := []struct {
		name             string
		createProfiles   []string
		expectedProfiles []string
		expectError      bool
	}{
		{
			name:             "list existing profiles",
			createProfiles:   []string{"local", "hdfs", "custom"},
			expectedProfiles: []string{"custom", "hdfs", "local"}, // Sorted
			expectError:      false,
		},
		{
			name:             "empty profiles directory",
			createProfiles:   []string{},
			expectedProfiles: []string{},
			expectError:      false,
		},
		{
			name:             "profiles with files (should ignore)",
			createProfiles:   []string{"local", "hdfs"},
			expectedProfiles: []string{"hdfs", "local"},
			expectError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			// Create profiles directory
			profilesDir := filepath.Join(baseDir, "conf", "profiles")
			util.MkdirAll(profilesDir)

			// Create test profiles
			for _, profile := range tt.createProfiles {
				util.MkdirAll(filepath.Join(profilesDir, profile))
			}

			// Create a file (not directory) to test filtering
			if len(tt.createProfiles) > 0 {
				os.WriteFile(filepath.Join(profilesDir, "notadir.txt"), []byte("test"), 0644)
			}

			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			profiles, err := pm.List()

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(profiles) != len(tt.expectedProfiles) {
				t.Errorf("Got %d profiles, want %d", len(profiles), len(tt.expectedProfiles))
				return
			}

			for i, profile := range profiles {
				if profile != tt.expectedProfiles[i] {
					t.Errorf("Profile[%d] = %q, want %q", i, profile, tt.expectedProfiles[i])
				}
			}
		})
	}
}

func TestProfileManager_Apply(t *testing.T) {
	tests := []struct {
		name        string
		profileName string
		fromRepo    bool
		expectError bool
	}{
		{
			name:        "apply local profile from repo",
			profileName: "local",
			fromRepo:    true,
			expectError: false,
		},
		{
			name:        "apply hdfs profile from repo",
			profileName: "hdfs",
			fromRepo:    true,
			expectError: false,
		},
		{
			name:        "apply non-existent profile",
			profileName: "nonexistent",
			fromRepo:    true,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			// Create repo profiles with test configs
			setupTestProfiles(t, repoRoot)

			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			err := pm.Apply(tt.profileName, tt.fromRepo)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify overlay was created
			currentConf := paths.CurrentConfDir()
			if !util.DirExists(currentConf) {
				t.Error("Current conf directory not created")
			}

			// Verify hive config exists (required)
			hiveConfig := filepath.Join(currentConf, "hive", "hive-site.xml")
			if !util.FileExists(hiveConfig) {
				t.Error("Hive config not created")
			}

			// Verify profile marker
			markerPath := filepath.Join(currentConf, ".profile")
			if !util.FileExists(markerPath) {
				t.Error("Profile marker not created")
			}

			markerContent, _ := os.ReadFile(markerPath)
			if string(markerContent) != tt.profileName {
				t.Errorf("Marker content = %q, want %q", string(markerContent), tt.profileName)
			}

			// For hdfs profile, verify hadoop configs exist
			if tt.profileName == "hdfs" {
				hadoopConfig := filepath.Join(currentConf, "hadoop", "core-site.xml")
				if !util.FileExists(hadoopConfig) {
					t.Error("Hadoop config not created for hdfs profile")
				}
			}

			// Verify template variables were replaced
			content, _ := os.ReadFile(hiveConfig)
			if len(content) == 0 {
				t.Error("Hive config is empty")
			}
			// Should not contain template markers if they were in source
			contentStr := string(content)
			if len(contentStr) > 0 && (contentStr[0:5] == "{{USER}}" || contentStr[0:7] == "{{HOME}}") {
				t.Error("Template variables not replaced")
			}
		})
	}
}

func TestProfileManager_Check(t *testing.T) {
	tests := []struct {
		name          string
		setupOverlay  bool
		includeHadoop bool
		expectError   bool
	}{
		{
			name:          "valid overlay with hadoop",
			setupOverlay:  true,
			includeHadoop: true,
			expectError:   false,
		},
		{
			name:          "valid overlay without hadoop",
			setupOverlay:  true,
			includeHadoop: false,
			expectError:   false,
		},
		{
			name:          "no overlay",
			setupOverlay:  false,
			includeHadoop: false,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			if tt.setupOverlay {
				currentConf := paths.CurrentConfDir()

				// Create required Hive config
				hiveDir := filepath.Join(currentConf, "hive")
				util.MkdirAll(hiveDir)
				os.WriteFile(filepath.Join(hiveDir, "hive-site.xml"), []byte("<configuration></configuration>"), 0644)

				// Optionally create Hadoop configs
				if tt.includeHadoop {
					hadoopDir := filepath.Join(currentConf, "hadoop")
					util.MkdirAll(hadoopDir)
					hadoopConfigs := []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"}
					for _, config := range hadoopConfigs {
						os.WriteFile(filepath.Join(hadoopDir, config), []byte("<configuration></configuration>"), 0644)
					}
				}
			}

			err := pm.Check()

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// Helper function to set up test profiles
func setupTestProfiles(t *testing.T, repoRoot string) {
	t.Helper()

	profiles := []struct {
		name       string
		hasHadoop  bool
		hasHive    bool
		hasSpark   bool
	}{
		{name: "local", hasHadoop: false, hasHive: true, hasSpark: true},
		{name: "hdfs", hasHadoop: true, hasHive: true, hasSpark: true},
	}

	for _, profile := range profiles {
		profileDir := filepath.Join(repoRoot, "conf", "profiles", profile.name)

		if profile.hasHadoop {
			hadoopDir := filepath.Join(profileDir, "hadoop")
			util.MkdirAll(hadoopDir)

			hadoopConfigs := []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"}
			for _, config := range hadoopConfigs {
				content := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>test.property</name>
    <value>{{USER}}</value>
  </property>
</configuration>`
				// Use .tmpl suffix to trigger template rendering
				os.WriteFile(filepath.Join(hadoopDir, config+".tmpl"), []byte(content), 0644)
			}
		}

		if profile.hasHive {
			hiveDir := filepath.Join(profileDir, "hive")
			util.MkdirAll(hiveDir)

			hiveContent := `<?xml version="1.0"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>{{BASE_DIR}}/state/hive/warehouse</value>
  </property>
</configuration>`
			os.WriteFile(filepath.Join(hiveDir, "hive-site.xml.tmpl"), []byte(hiveContent), 0644)
		}

		if profile.hasSpark {
			sparkDir := filepath.Join(profileDir, "spark")
			util.MkdirAll(sparkDir)

			sparkContent := `spark.master=local[*]
spark.home={{HOME}}/spark`
			os.WriteFile(filepath.Join(sparkDir, "spark-defaults.conf.tmpl"), []byte(sparkContent), 0644)
		}
	}
}
