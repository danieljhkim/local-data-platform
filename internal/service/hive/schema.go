package hive

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/env"
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
// Returns SchemaInitialized if schema exists, SchemaNotInitialized if not, SchemaUnknown on error.
// A canceled ctx stops schematool and is returned as ctx.Err() rather than being
// interpreted as a schema status.
func (h *HiveService) checkMetastoreSchema(ctx context.Context, dbType metastore.DBType) (SchemaStatus, error) {
	cmd, err := env.CommandContext(ctx, "schematool", []string{"-dbType", string(dbType), "-info"}, h.env.Export())
	if err != nil {
		return SchemaUnknown, err
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return SchemaUnknown, ctxErr
	}
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

// initMetastoreSchema initializes the Hive metastore schema. A canceled ctx
// stops schematool and is returned as ctx.Err().
func (h *HiveService) initMetastoreSchema(ctx context.Context, dbType metastore.DBType) error {
	util.Log("Initializing Hive metastore schema...")

	cmd, err := env.CommandContext(ctx, "schematool", []string{"-dbType", string(dbType), "-initSchema"}, h.env.Export())
	if err != nil {
		return err
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	output := stdout.String() + stderr.String()

	if err != nil {
		return fmt.Errorf("failed to initialize metastore schema: %v\nOutput: %s", err, strings.TrimSpace(output))
	}

	// Check for success message
	if strings.Contains(output, "Initialization script completed") ||
		strings.Contains(output, "schemaTool completed") {
		util.Success("Metastore schema initialized successfully")
		return nil
	}

	// Command succeeded without error
	util.Log("Metastore schema initialization completed")
	return nil
}

func (h *HiveService) ensureMetastoreSchema(ctx context.Context) error {
	dbType, _, err := h.detectMetastoreConfig()
	if err != nil {
		return err
	}
	return h.ensureMetastoreSchemaForType(ctx, dbType, false)
}

// ensureMetastoreSchemaForType checks and, when needed, initializes the
// metastore schema. Cancellation is always fatal: the non-strict path only
// downgrades schematool failures, never an abandoned start.
func (h *HiveService) ensureMetastoreSchemaForType(ctx context.Context, dbType metastore.DBType, strict bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := env.CheckHiveJava(h.env); err != nil {
		return err
	}
	util.Log("Checking Hive metastore schema...")

	status, err := h.checkMetastoreSchema(ctx, dbType)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
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
		if err := h.initMetastoreSchema(ctx, dbType); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
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
	return h.ensureMetastoreSchemaForType(context.Background(), dbType, true)
}
