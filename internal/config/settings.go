package config

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

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
	var settings *Settings
	err := withConfigLock(sm.paths, func() error {
		var err error
		settings, err = sm.loadUnlocked()
		return err
	})
	return settings, err
}

func (sm *SettingsManager) loadUnlocked() (*Settings, error) {
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
	return withConfigLock(sm.paths, func() error {
		return sm.saveAndApplyUnlocked(settings)
	})
}

// UpdateAndApply serializes a read-modify-write setting change and publishes
// the settings JSON together with every existing Hive XML copy.
func (sm *SettingsManager) UpdateAndApply(update func(*Settings) error) error {
	if update == nil {
		return fmt.Errorf("settings update required")
	}
	return withConfigLock(sm.paths, func() error {
		settings, err := sm.loadOrDefaultUnlocked()
		if err != nil {
			return err
		}
		if err := update(settings); err != nil {
			return err
		}
		return sm.saveAndApplyUnlocked(settings)
	})
}

func (sm *SettingsManager) saveAndApplyUnlocked(settings *Settings) error {
	transactionID := newTransactionID()
	settingsEntry, err := sm.stageSettings(settings, transactionID)
	if err != nil {
		return err
	}

	xmlEntries, err := NewSettingsApplier(sm.paths).stageHiveSettings(settings, transactionID)
	if err != nil {
		_ = os.RemoveAll(settingsEntry.Stage)
		return err
	}
	// Publish XML first and settings last. Locked readers observe either the
	// complete old set or the complete new set, and rollback covers any error.
	entries := append(xmlEntries, settingsEntry)
	return publishStaged(sm.paths, transactionID, entries)
}

func (sm *SettingsManager) stageSettings(settings *Settings, transactionID string) (stagedReplacement, error) {
	if settings == nil {
		return stagedReplacement{}, fmt.Errorf("settings required")
	}
	// base-dir is static and derived from runtime paths.
	settings.BaseDir = sm.paths.BaseDir
	if err := sm.sanitize(settings); err != nil {
		return stagedReplacement{}, err
	}

	if err := os.MkdirAll(sm.paths.SettingsDir(), 0755); err != nil {
		return stagedReplacement{}, err
	}

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return stagedReplacement{}, fmt.Errorf("failed to marshal settings: %w", err)
	}

	entry := newStagedReplacement(sm.Path(), "settings", transactionID)
	if err := sm.paths.runConfigHook("settings.write"); err != nil {
		return stagedReplacement{}, err
	}
	if err := util.WriteFile(entry.Stage, append(data, '\n'), util.PrivateFileMode); err != nil {
		_ = os.Remove(entry.Stage)
		return stagedReplacement{}, err
	}
	if err := syncFile(entry.Stage); err != nil {
		_ = os.Remove(entry.Stage)
		return stagedReplacement{}, err
	}
	return entry, nil
}

// LoadOrDefault reads settings if available, otherwise returns runtime defaults.
func (sm *SettingsManager) LoadOrDefault() (*Settings, error) {
	var settings *Settings
	err := withConfigLock(sm.paths, func() error {
		var err error
		settings, err = sm.loadOrDefaultUnlocked()
		return err
	})
	return settings, err
}

func (sm *SettingsManager) loadOrDefaultUnlocked() (*Settings, error) {
	settings, err := sm.loadUnlocked()
	if err == nil {
		return settings, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	return defaultSettings(sm.paths.BaseDir), nil
}

func relativeConfigPath(paths *Paths, path string) string {
	rel, err := filepath.Rel(paths.BaseDir, path)
	if err != nil {
		return filepath.Base(path)
	}
	return filepath.ToSlash(rel)
}

func defaultSettings(baseDir string) *Settings {
	dbType := metastore.Derby
	return &Settings{
		User:    runtimeUser(),
		BaseDir: baseDir,
		DBType:  string(dbType),
		DBURL:   metastore.DefaultDBURLForBase(dbType, baseDir),
		// Derby's embedded "APP" user ignores this value, so an empty default
		// is safe. Postgres/MySQL never fall back to this: cli/init.go
		// requires an explicit password or explicit empty-password consent
		// before persisting settings for an external database.
		DBPassword: "",
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
