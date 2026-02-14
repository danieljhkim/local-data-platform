package hive

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/metastore"
	"github.com/danieljhkim/local-data-platform/internal/util"
)

var dbIdentPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type mysqlConnInfo struct {
	host     string
	port     string
	user     string
	password string
	dbName   string
}

// BootstrapMetastore prepares metastore dependencies and initializes schema.
func (h *HiveService) BootstrapMetastore(in io.Reader, out, errOut io.Writer) error {
	dbType, dbURL, err := h.detectMetastoreConfig()
	if err != nil {
		return err
	}

	if err := h.ensureJDBCDriver(dbType); err != nil {
		return err
	}

	if err := h.ensureDatabaseExists(dbType, dbURL, in, out, errOut); err != nil {
		return err
	}

	return h.ensureMetastoreSchemaStrict(dbType)
}

func (h *HiveService) detectMetastoreConfig() (metastore.DBType, string, error) {
	hiveSite := filepath.Join(h.env.HiveConfDir, "hive-site.xml")
	cfg, err := util.ParseHadoopXML(hiveSite)
	if err != nil {
		return metastore.Derby, "", fmt.Errorf("failed to parse hive metastore config %s: %w", hiveSite, err)
	}

	dbURL := strings.TrimSpace(cfg.GetProperty("javax.jdo.option.ConnectionURL"))
	driver := strings.ToLower(strings.TrimSpace(cfg.GetProperty("javax.jdo.option.ConnectionDriverName")))
	dbType := metastore.InferDBTypeFromURL(dbURL)

	if dbType == "" {
		switch {
		case strings.Contains(driver, "postgres"):
			dbType = metastore.Postgres
		case strings.Contains(driver, "mysql"):
			dbType = metastore.MySQL
		default:
			dbType = metastore.Derby
		}
	}
	if dbURL == "" {
		dbURL = metastore.DefaultDBURL(dbType)
	}

	h.usesPostgresMetastore = dbType == metastore.Postgres
	return dbType, dbURL, nil
}

func (h *HiveService) ensureJDBCDriver(dbType metastore.DBType) error {
	switch dbType {
	case metastore.Postgres:
		util.Log("Postgres metastore detected, ensuring JDBC driver is available...")
		_, err := EnsurePostgresJDBCDriver(h.env.HiveHome, h.env.SparkHome, h.paths.BaseDir)
		if err != nil {
			return fmt.Errorf("failed to ensure Postgres JDBC driver: %w", err)
		}
	case metastore.MySQL:
		util.Log("MySQL metastore detected, ensuring JDBC driver is available...")
		_, err := EnsureMySQLJDBCDriver(h.env.HiveHome, h.env.SparkHome, h.paths.BaseDir)
		if err != nil {
			return fmt.Errorf("failed to ensure MySQL JDBC driver: %w", err)
		}
	}
	return nil
}

func (h *HiveService) ensureDatabaseExists(dbType metastore.DBType, dbURL string, in io.Reader, out, errOut io.Writer) error {
	switch dbType {
	case metastore.Derby:
		return nil
	case metastore.Postgres, metastore.MySQL:
		exists, err := h.databaseExists(dbType, dbURL)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}

		fmt.Fprintf(errOut, "WARNING: %s metastore database not found for URL: %s\n", dbType, dbURL)
		create, err := confirmYesNo(in, out, "Create metastore database now? [y/N]: ")
		if err != nil {
			return err
		}
		if !create {
			return fmt.Errorf("%s metastore database does not exist", dbType)
		}
		return h.createDatabase(dbType, dbURL)
	default:
		return nil
	}
}

func confirmYesNo(in io.Reader, out io.Writer, prompt string) (bool, error) {
	fmt.Fprint(out, prompt)
	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func (h *HiveService) databaseExists(dbType metastore.DBType, dbURL string) (bool, error) {
	switch dbType {
	case metastore.Postgres:
		adminURL, dbName, err := parsePostgresURL(dbURL)
		if err != nil {
			return false, err
		}
		sql := fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname='%s';", escapeSQLLiteral(dbName))
		cmd := exec.Command("psql", adminURL, "-tAc", sql)
		cmd.Env = h.env.Export()
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, fmt.Errorf("postgres database existence check failed: %v\nOutput: %s", err, strings.TrimSpace(string(out)))
		}
		return strings.TrimSpace(string(out)) == "1", nil
	case metastore.MySQL:
		info, err := parseMySQLURL(dbURL)
		if err != nil {
			return false, err
		}
		args := mysqlBaseArgs(info)
		query := fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='%s';", escapeSQLLiteral(info.dbName))
		args = append(args, "-e", query)
		cmd := exec.Command("mysql", args...)
		cmd.Env = h.env.Export()
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, fmt.Errorf("mysql database existence check failed: %v\nOutput: %s", err, strings.TrimSpace(string(out)))
		}
		return strings.TrimSpace(string(out)) == info.dbName, nil
	default:
		return true, nil
	}
}

func (h *HiveService) createDatabase(dbType metastore.DBType, dbURL string) error {
	switch dbType {
	case metastore.Postgres:
		adminURL, dbName, err := parsePostgresURL(dbURL)
		if err != nil {
			return err
		}
		if !dbIdentPattern.MatchString(dbName) {
			return fmt.Errorf("unsupported postgres database name %q", dbName)
		}
		cmd := exec.Command("psql", adminURL, "-c", fmt.Sprintf("CREATE DATABASE %s;", dbName))
		cmd.Env = h.env.Export()
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to create postgres database %q: %v\nOutput: %s", dbName, err, strings.TrimSpace(string(out)))
		}
		util.Log("Created Postgres metastore database %q", dbName)
		return nil
	case metastore.MySQL:
		info, err := parseMySQLURL(dbURL)
		if err != nil {
			return err
		}
		if !dbIdentPattern.MatchString(info.dbName) {
			return fmt.Errorf("unsupported mysql database name %q", info.dbName)
		}
		args := mysqlBaseArgs(info)
		args = append(args, "-e", fmt.Sprintf("CREATE DATABASE `%s`;", info.dbName))
		cmd := exec.Command("mysql", args...)
		cmd.Env = h.env.Export()
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to create mysql database %q: %v\nOutput: %s", info.dbName, err, strings.TrimSpace(string(out)))
		}
		util.Log("Created MySQL metastore database %q", info.dbName)
		return nil
	default:
		return nil
	}
}

func parsePostgresURL(dbURL string) (string, string, error) {
	raw := strings.TrimSpace(dbURL)
	if !strings.HasPrefix(strings.ToLower(raw), "jdbc:postgresql://") {
		return "", "", fmt.Errorf("invalid postgres db-url %q", dbURL)
	}
	u, err := url.Parse(strings.TrimPrefix(raw, "jdbc:"))
	if err != nil {
		return "", "", fmt.Errorf("failed to parse postgres db-url: %w", err)
	}

	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return "", "", fmt.Errorf("postgres db-url missing database name: %q", dbURL)
	}
	admin := *u
	admin.Path = "/postgres"
	admin.RawQuery = ""
	return admin.String(), dbName, nil
}

func parseMySQLURL(dbURL string) (*mysqlConnInfo, error) {
	raw := strings.TrimSpace(dbURL)
	if !strings.HasPrefix(strings.ToLower(raw), "jdbc:mysql://") {
		return nil, fmt.Errorf("invalid mysql db-url %q", dbURL)
	}
	u, err := url.Parse(strings.TrimPrefix(raw, "jdbc:"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse mysql db-url: %w", err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return nil, fmt.Errorf("mysql db-url missing database name: %q", dbURL)
	}

	password, _ := u.User.Password()
	return &mysqlConnInfo{
		host:     defaultString(u.Hostname(), "localhost"),
		port:     defaultString(u.Port(), "3306"),
		user:     u.User.Username(),
		password: password,
		dbName:   dbName,
	}, nil
}

func mysqlBaseArgs(info *mysqlConnInfo) []string {
	args := []string{
		"--batch",
		"--skip-column-names",
		"--host", info.host,
		"--port", info.port,
	}
	if info.user != "" {
		args = append(args, "--user", info.user)
	}
	if info.password != "" {
		args = append(args, fmt.Sprintf("--password=%s", info.password))
	}
	return args
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
