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

type sqlConnInfo struct {
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

		if _, err := fmt.Fprintf(errOut, "WARNING: %s metastore database not found for URL: %s\n", dbType, util.RedactJDBCURL(dbURL)); err != nil {
			return err
		}
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
	if _, err := fmt.Fprint(out, prompt); err != nil {
		return false, err
	}
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
		info, err := parsePostgresURL(dbURL)
		if err != nil {
			return false, err
		}
		if !dbIdentPattern.MatchString(info.dbName) {
			return false, fmt.Errorf("unsupported postgres database name %q", info.dbName)
		}
		sql := fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname='%s';", escapeSQLLiteral(info.dbName))
		args := append(postgresBaseArgs(info, "postgres"), "-tAc", sql)
		cmd := exec.Command("psql", args...)
		cmd.Env = postgresCmdEnv(h.env.Export(), info)
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
		if !dbIdentPattern.MatchString(info.dbName) {
			return false, fmt.Errorf("unsupported mysql database name %q", info.dbName)
		}
		args := mysqlBaseArgs(info)
		query := fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='%s';", escapeSQLLiteral(info.dbName))
		args = append(args, "-e", query)
		cmd := exec.Command("mysql", args...)
		cmd.Env = mysqlCmdEnv(h.env.Export(), info)
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
		info, err := parsePostgresURL(dbURL)
		if err != nil {
			return err
		}
		if !dbIdentPattern.MatchString(info.dbName) {
			return fmt.Errorf("unsupported postgres database name %q", info.dbName)
		}
		args := append(postgresBaseArgs(info, "postgres"), "-c", fmt.Sprintf("CREATE DATABASE %s;", info.dbName))
		cmd := exec.Command("psql", args...)
		cmd.Env = postgresCmdEnv(h.env.Export(), info)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to create postgres database %q: %v\nOutput: %s", info.dbName, err, strings.TrimSpace(string(out)))
		}
		util.Log("Created Postgres metastore database %q", info.dbName)
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
		cmd.Env = mysqlCmdEnv(h.env.Export(), info)
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

func parsePostgresURL(dbURL string) (*sqlConnInfo, error) {
	u, dbName, err := parseJDBCURL(dbURL, "postgresql")
	if err != nil {
		return nil, err
	}
	info := connInfoFromURL(u, dbName, "localhost", "5432")
	return info, nil
}

func parseMySQLURL(dbURL string) (*sqlConnInfo, error) {
	u, dbName, err := parseJDBCURL(dbURL, "mysql")
	if err != nil {
		return nil, err
	}
	return connInfoFromURL(u, dbName, "localhost", "3306"), nil
}

func parseJDBCURL(dbURL, driver string) (*url.URL, string, error) {
	raw := strings.TrimSpace(dbURL)
	prefix := "jdbc:" + driver + "://"
	if !strings.HasPrefix(strings.ToLower(raw), prefix) {
		return nil, "", fmt.Errorf("invalid %s db-url %q", driver, util.RedactJDBCURL(dbURL))
	}
	rest := raw
	if len(raw) >= 5 && strings.EqualFold(raw[:5], "jdbc:") {
		rest = raw[5:]
	}
	u, err := url.Parse(rest)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse %s db-url: %w", driver, err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return nil, "", fmt.Errorf("%s db-url missing database name: %q", driver, util.RedactJDBCURL(dbURL))
	}
	return u, dbName, nil
}

func connInfoFromURL(u *url.URL, dbName, defaultHost, defaultPort string) *sqlConnInfo {
	user := ""
	password := ""
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
	}
	q := u.Query()
	if user == "" {
		user = firstNonEmpty(q.Get("user"), q.Get("username"))
	}
	if password == "" {
		password = firstNonEmpty(q.Get("password"), q.Get("pwd"), q.Get("passwd"))
	}
	return &sqlConnInfo{
		host:     defaultString(u.Hostname(), defaultHost),
		port:     defaultString(u.Port(), defaultPort),
		user:     user,
		password: password,
		dbName:   dbName,
	}
}

func postgresBaseArgs(info *sqlConnInfo, database string) []string {
	args := make([]string, 0, 8)
	if info.host != "" {
		args = append(args, "-h", info.host)
	}
	if info.port != "" {
		args = append(args, "-p", info.port)
	}
	if info.user != "" {
		args = append(args, "-U", info.user)
	}
	if database != "" {
		args = append(args, "-d", database)
	}
	return args
}

func postgresCmdEnv(baseEnv []string, info *sqlConnInfo) []string {
	return withPasswordEnv(baseEnv, "PGPASSWORD", info.password)
}

func mysqlBaseArgs(info *sqlConnInfo) []string {
	args := []string{
		"--batch",
		"--skip-column-names",
		"--host", info.host,
		"--port", info.port,
	}
	if info.user != "" {
		args = append(args, "--user", info.user)
	}
	// Password is passed via MYSQL_PWD env var in mysqlCmdEnv() to avoid
	// exposing it in process listings (ps aux).
	return args
}

// mysqlCmdEnv returns environment variables for MySQL commands,
// including MYSQL_PWD if a password is set.
func mysqlCmdEnv(baseEnv []string, info *sqlConnInfo) []string {
	return withPasswordEnv(baseEnv, "MYSQL_PWD", info.password)
}

func withPasswordEnv(baseEnv []string, key, password string) []string {
	if password == "" {
		return baseEnv
	}
	prefix := key + "="
	out := make([]string, 0, len(baseEnv)+1)
	for _, e := range baseEnv {
		if strings.HasPrefix(e, prefix) {
			continue
		}
		out = append(out, e)
	}
	return append(out, prefix+password)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
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
