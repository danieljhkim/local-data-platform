package hive

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

// SchemaStatus represents the result of checking metastore schema
type SchemaStatus int

const (
	SchemaUnknown SchemaStatus = iota
	SchemaNotInitialized
	SchemaInitialized
)

// checkMetastoreSchema checks if the Hive metastore schema is initialized
// Returns SchemaInitialized if schema exists, SchemaNotInitialized if not, SchemaUnknown on error
func (h *HiveService) checkMetastoreSchema(dbType metastore.DBType) (SchemaStatus, error) {
	cmd := exec.Command("schematool", "-dbType", string(dbType), "-info")
	cmd.Env = h.env.Export()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String() + stderr.String()

	// schematool -info returns non-zero if schema is not initialized
	if err != nil {
		// Check for common "schema not found" or "relation does not exist" messages
		if strings.Contains(output, "does not exist") ||
			strings.Contains(output, "relation") ||
			strings.Contains(output, "Table") ||
			strings.Contains(output, "not exist") ||
			strings.Contains(output, "Schema initialization") {
			return SchemaNotInitialized, nil
		}

		// Connection errors or other issues
		if strings.Contains(output, "Connection refused") ||
			strings.Contains(output, "FATAL") ||
			strings.Contains(output, "password authentication failed") {
			return SchemaUnknown, fmt.Errorf("database connection error: %s", strings.TrimSpace(output))
		}

		// Other unknown error
		return SchemaUnknown, fmt.Errorf("schematool -info failed: %v\nOutput: %s", err, strings.TrimSpace(output))
	}

	// Success - schema is initialized
	// Look for "Hive distribution version" or similar success indicators
	if strings.Contains(output, "Hive distribution version") ||
		strings.Contains(output, "Metastore schema version") {
		return SchemaInitialized, nil
	}

	// If command succeeded but output is unexpected, assume initialized
	return SchemaInitialized, nil
}

// initMetastoreSchema initializes the Hive metastore schema
func (h *HiveService) initMetastoreSchema(dbType metastore.DBType) error {
	util.Log("Initializing Hive metastore schema...")

	cmd := exec.Command("schematool", "-dbType", string(dbType), "-initSchema")
	cmd.Env = h.env.Export()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	output := stdout.String() + stderr.String()

	if err != nil {
		return fmt.Errorf("failed to initialize metastore schema: %v\nOutput: %s", err, strings.TrimSpace(output))
	}

	// Check for success message
	if strings.Contains(output, "Initialization script completed") ||
		strings.Contains(output, "schemaTool completed") {
		util.Log("Metastore schema initialized successfully")
		return nil
	}

	// Command succeeded without error
	util.Log("Metastore schema initialization completed")
	return nil
}

func (h *HiveService) ensureMetastoreSchema() error {
	dbType, _, err := h.detectMetastoreConfig()
	if err != nil {
		return err
	}
	return h.ensureMetastoreSchemaForType(dbType, false)
}

func (h *HiveService) ensureMetastoreSchemaForType(dbType metastore.DBType, strict bool) error {
	util.Log("Checking Hive metastore schema...")

	status, err := h.checkMetastoreSchema(dbType)
	if err != nil {
		if strict {
			return err
		}
		util.Warn("Could not check metastore schema: %v", err)
		util.Warn("Will attempt to start metastore anyway")
		return nil
	}

	switch status {
	case SchemaInitialized:
		util.Log("Metastore schema is initialized")
		return nil

	case SchemaNotInitialized:
		util.Log("Metastore schema not found, initializing...")
		if err := h.initMetastoreSchema(dbType); err != nil {
			if strict {
				return err
			}
			util.Warn("Failed to initialize metastore schema: %v", err)
			util.Warn("Will attempt to start metastore anyway")
			return nil
		}
		return nil

	default:
		if strict {
			return fmt.Errorf("could not determine metastore schema status")
		}
		util.Warn("Could not determine schema status, will attempt to start metastore")
		return nil
	}
}

func (h *HiveService) ensureMetastoreSchemaStrict(dbType metastore.DBType) error {
	return h.ensureMetastoreSchemaForType(dbType, true)
}

func (h *HiveService) isPostgresMetastore() bool {
	return h.usesPostgresMetastore
}
