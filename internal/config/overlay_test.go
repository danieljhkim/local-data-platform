package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danieljhkim/local-data-platform/internal/config/generator"
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
			err := pm.Init(tt.force, nil)

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

			// Otherwise, verify user profiles directory exists and profiles were generated
			if !util.DirExists(userProfiles) {
				t.Error("User profiles directory not created")
			}

			// Verify profiles were generated
			if !util.DirExists(filepath.Join(userProfiles, "local")) {
				t.Error("Local profile not generated")
			}
			if !util.DirExists(filepath.Join(userProfiles, "hdfs")) {
				t.Error("HDFS profile not generated")
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

func TestProfileManager_InitWithOptionsPreservedOnSet(t *testing.T) {
	tmpDir := t.TempDir()
	repoRoot := filepath.Join(tmpDir, "repo")
	baseDir := filepath.Join(tmpDir, "base")

	paths := NewPaths(repoRoot, baseDir)
	pm := NewProfileManager(paths)

	// Custom DB options
	customDBUrl := "jdbc:postgresql://custom-host:5432/mydb"
	customDBPassword := "mypassword"

	opts := &generator.InitOptions{
		DBUrl:      customDBUrl,
		DBPassword: customDBPassword,
	}

	// Init with custom options
	if err := pm.Init(false, opts); err != nil {
		t.Fatalf("Failed to init profiles: %v", err)
	}

	// Verify custom values in profile directory
	profileHiveConfig := filepath.Join(paths.UserProfilesDir(), "hdfs", "hive", "hive-site.xml")
	content, err := os.ReadFile(profileHiveConfig)
	if err != nil {
		t.Fatalf("Failed to read profile hive-site.xml: %v", err)
	}
	if !strings.Contains(string(content), customDBUrl) {
		t.Errorf("Profile hive-site.xml should contain custom DB URL %q", customDBUrl)
	}
	if !strings.Contains(string(content), customDBPassword) {
		t.Errorf("Profile hive-site.xml should contain custom DB password")
	}

	// Now set the profile (this should preserve the custom values)
	if err := pm.Set("hdfs"); err != nil {
		t.Fatalf("Failed to set profile: %v", err)
	}

	// Verify custom values are preserved in current overlay
	currentHiveConfig := filepath.Join(paths.CurrentConfDir(), "hive", "hive-site.xml")
	currentContent, err := os.ReadFile(currentHiveConfig)
	if err != nil {
		t.Fatalf("Failed to read current hive-site.xml: %v", err)
	}

	if !strings.Contains(string(currentContent), customDBUrl) {
		t.Errorf("Current hive-site.xml should contain custom DB URL %q\nContent: %s", customDBUrl, string(currentContent))
	}
	if !strings.Contains(string(currentContent), customDBPassword) {
		t.Errorf("Current hive-site.xml should contain custom DB password\nContent: %s", string(currentContent))
	}
}

func TestProfileManager_List(t *testing.T) {
	tests := []struct {
		name             string
		initProfiles     bool
		expectedProfiles []string
		expectError      bool
	}{
		{
			name:             "list after init",
			initProfiles:     true,
			expectedProfiles: []string{"hdfs", "local"}, // Sorted
			expectError:      false,
		},
		{
			name:             "list without init",
			initProfiles:     false,
			expectedProfiles: nil,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			if tt.initProfiles {
				if err := pm.Init(false, nil); err != nil {
					t.Fatalf("Failed to init profiles: %v", err)
				}
			}

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
		expectError bool
	}{
		{
			name:        "apply local profile",
			profileName: "local",
			expectError: false,
		},
		{
			name:        "apply hdfs profile",
			profileName: "hdfs",
			expectError: false,
		},
		{
			name:        "apply non-existent profile",
			profileName: "nonexistent",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			repoRoot := filepath.Join(tmpDir, "repo")
			baseDir := filepath.Join(tmpDir, "base")

			paths := NewPaths(repoRoot, baseDir)
			pm := NewProfileManager(paths)

			// Initialize profiles first (Apply now copies from conf/profiles/)
			if err := pm.Init(false, nil); err != nil {
				t.Fatalf("Failed to init profiles: %v", err)
			}

			err := pm.Apply(tt.profileName)

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

			// For local profile, verify no hadoop configs
			if tt.profileName == "local" {
				hadoopDir := filepath.Join(currentConf, "hadoop")
				if util.DirExists(hadoopDir) {
					t.Error("Hadoop directory should not exist for local profile")
				}
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
