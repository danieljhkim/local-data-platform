package config

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/metastore"
)

const defaultDBPassword = "password"

// Settings holds persisted user-configurable settings.
type Settings struct {
	User       string `json:"user"`
	BaseDir    string `json:"base-dir"`
	DBType     string `json:"db-type"`
	DBURL      string `json:"db-url"`
	DBPassword string `json:"db-password"`
}

// SettingsManager handles settings persistence.
type SettingsManager struct {
	paths *Paths
}

// NewSettingsManager creates a settings manager.
func NewSettingsManager(paths *Paths) *SettingsManager {
	return &SettingsManager{paths: paths}
}

// Path returns the settings file path.
func (sm *SettingsManager) Path() string {
	return sm.paths.SettingsFile()
}

// Load reads settings from disk.
func (sm *SettingsManager) Load() (*Settings, error) {
	data, err := os.ReadFile(sm.Path())
	if err != nil {
		return nil, err
	}

	var settings Settings
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("failed to parse settings: %w", err)
	}
	if err := sm.sanitize(&settings); err != nil {
		return nil, err
	}
	// base-dir is static and derived from runtime paths.
	settings.BaseDir = sm.paths.BaseDir

	return &settings, nil
}

// Save writes settings to disk.
func (sm *SettingsManager) Save(settings *Settings) error {
	if settings == nil {
		return fmt.Errorf("settings required")
	}
	// base-dir is static and derived from runtime paths.
	settings.BaseDir = sm.paths.BaseDir
	if err := sm.sanitize(settings); err != nil {
		return err
	}

	if err := os.MkdirAll(sm.paths.SettingsDir(), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}

	if err := os.WriteFile(sm.Path(), append(data, '\n'), 0644); err != nil {
		return err
	}

	return nil
}

// LoadOrDefault reads settings if available, otherwise returns runtime defaults.
func (sm *SettingsManager) LoadOrDefault() (*Settings, error) {
	settings, err := sm.Load()
	if err == nil {
		return settings, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	return defaultSettings(sm.paths.BaseDir), nil
}

func defaultSettings(baseDir string) *Settings {
	dbType := metastore.Derby
	return &Settings{
		User:       runtimeUser(),
		BaseDir:    baseDir,
		DBType:     string(dbType),
		DBURL:      metastore.DefaultDBURLForBase(dbType, baseDir),
		DBPassword: defaultDBPassword,
	}
}

func (sm *SettingsManager) sanitize(settings *Settings) error {
	if settings == nil {
		return fmt.Errorf("settings required")
	}
	settings.User = strings.TrimSpace(settings.User)
	if settings.User == "" {
		settings.User = runtimeUser()
	}

	settings.DBURL = strings.TrimSpace(settings.DBURL)
	settings.DBPassword = strings.TrimSpace(settings.DBPassword)
	if settings.DBPassword == "" {
		settings.DBPassword = defaultDBPassword
	}

	rawType := strings.TrimSpace(settings.DBType)
	if rawType == "" {
		if inferred := metastore.InferDBTypeFromURL(settings.DBURL); inferred != "" {
			rawType = string(inferred)
		}
	}
	dbType, err := metastore.NormalizeDBType(rawType)
	if err != nil {
		return err
	}
	settings.DBType = string(dbType)

	if settings.DBURL == "" {
		settings.DBURL = metastore.DefaultDBURLForBase(dbType, sm.paths.BaseDir)
		return nil
	}
	if dbType == metastore.Derby && settings.DBURL == metastore.DefaultDBURL(metastore.Derby) {
		// Migrate legacy relative Derby path to base-dir-scoped absolute path.
		settings.DBURL = metastore.DefaultDBURLForBase(dbType, sm.paths.BaseDir)
	}
	return nil
}

func runtimeUser() string {
	if u, err := user.Current(); err == nil && strings.TrimSpace(u.Username) != "" {
		return u.Username
	}
	return strings.TrimSpace(os.Getenv("USER"))
}
