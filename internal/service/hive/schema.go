package hive

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

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
func (h *HiveService) checkMetastoreSchema() (SchemaStatus, error) {
	cmd := exec.Command("schematool", "-dbType", "postgres", "-info")
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
func (h *HiveService) initMetastoreSchema() error {
	util.Log("Initializing Hive metastore schema...")

	cmd := exec.Command("schematool", "-dbType", "postgres", "-initSchema")
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

// ensureMetastoreSchema checks if schema is initialized and initializes if needed
// Only runs for Postgres metastore configurations
func (h *HiveService) ensureMetastoreSchema() error {
	// First check if this is a Postgres metastore
	if !h.isPostgresMetastore() {
		return nil
	}

	util.Log("Checking Hive metastore schema...")

	status, err := h.checkMetastoreSchema()
	if err != nil {
		// Log warning but don't fail - metastore might still work
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
		if err := h.initMetastoreSchema(); err != nil {
			return err
		}
		return nil

	default:
		util.Warn("Could not determine schema status, will attempt to start metastore")
		return nil
	}
}

// isPostgresMetastore checks if the current config uses Postgres metastore
func (h *HiveService) isPostgresMetastore() bool {
	// This is already checked in ensurePostgresJDBC, we can reuse the logic
	// by checking for org.postgresql.Driver in hive-site.xml
	return h.usesPostgresMetastore
}
