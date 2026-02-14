package metastore

import (
	"fmt"
	"path/filepath"
	"strings"
)

// DBType identifies the Hive metastore backing database.
type DBType string

const (
	Derby    DBType = "derby"
	Postgres DBType = "postgres"
	MySQL    DBType = "mysql"
)

const (
	defaultDerbyDBURL    = "jdbc:derby:;databaseName=metastore_db;create=true"
	defaultPostgresDBURL = "jdbc:postgresql://localhost:5432/metastore"
	defaultMySQLDBURL    = "jdbc:mysql://localhost:3306/metastore"
)

// NormalizeDBType parses and validates db type values.
func NormalizeDBType(value string) (DBType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(Derby):
		return Derby, nil
	case string(Postgres):
		return Postgres, nil
	case string(MySQL):
		return MySQL, nil
	default:
		return "", fmt.Errorf("unknown db-type %q (supported: derby, postgres, mysql)", value)
	}
}

// InferDBTypeFromURL infers db type from JDBC URL prefix.
func InferDBTypeFromURL(dbURL string) DBType {
	u := strings.ToLower(strings.TrimSpace(dbURL))
	switch {
	case strings.HasPrefix(u, "jdbc:postgresql:"):
		return Postgres
	case strings.HasPrefix(u, "jdbc:mysql:"):
		return MySQL
	case strings.HasPrefix(u, "jdbc:derby:"):
		return Derby
	default:
		return ""
	}
}

func DefaultDBURL(dbType DBType) string {
	switch dbType {
	case Postgres:
		return defaultPostgresDBURL
	case MySQL:
		return defaultMySQLDBURL
	default:
		return defaultDerbyDBURL
	}
}

func DefaultDBURLForBase(dbType DBType, baseDir string) string {
	if dbType != Derby {
		return DefaultDBURL(dbType)
	}
	if strings.TrimSpace(baseDir) == "" {
		return DefaultDBURL(dbType)
	}
	derbyDBPath := filepath.ToSlash(filepath.Join(baseDir, "state", "hive", "metastore_db"))
	return fmt.Sprintf("jdbc:derby:;databaseName=%s;create=true", derbyDBPath)
}

func DriverClass(dbType DBType) string {
	switch dbType {
	case Postgres:
		return "org.postgresql.Driver"
	case MySQL:
		return "com.mysql.cj.jdbc.Driver"
	default:
		return "org.apache.derby.iapi.jdbc.AutoloadedDriver"
	}
}

func ConnectionUser(dbType DBType, configuredUser string) string {
	if dbType == Derby {
		return "APP"
	}
	if strings.TrimSpace(configuredUser) == "" {
		return "APP"
	}
	return configuredUser
}

func ValidateURL(dbType DBType, dbURL string) error {
	urlType := InferDBTypeFromURL(dbURL)
	if urlType == "" {
		return fmt.Errorf("db-url %q is not a supported JDBC URL (expected derby, postgres, or mysql)", dbURL)
	}
	if urlType != dbType {
		return fmt.Errorf("db-type %q does not match db-url %q", dbType, dbURL)
	}
	return nil
}
