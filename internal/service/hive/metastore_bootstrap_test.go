package hive

import (
	"os/exec"
	"strings"
	"testing"
)

const testSecret = "s3cret-value"

func assertNoSecretInArgs(t *testing.T, args []string) {
	t.Helper()
	for _, arg := range args {
		if strings.Contains(arg, testSecret) {
			t.Fatalf("secret leaked in argv %q", arg)
		}
	}
}

func TestParsePostgresURL_UserinfoPasswordGoesToEnvNotArgs(t *testing.T) {
	info, err := parsePostgresURL("jdbc:postgresql://alice:" + testSecret + "@localhost:5432/metastore")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if info.user != "alice" || info.password != testSecret || info.dbName != "metastore" {
		t.Fatalf("parsed %+v", info)
	}

	args := append(postgresBaseArgs(info, "postgres"), "-tAc", "SELECT 1 FROM pg_database WHERE datname='metastore';")
	cmd := exec.Command("psql", args...)
	cmd.Env = postgresCmdEnv([]string{"PATH=/usr/bin"}, info)

	assertNoSecretInArgs(t, cmd.Args)
	found := false
	for _, e := range cmd.Env {
		if strings.Contains(e, testSecret) && e != "PGPASSWORD="+testSecret {
			t.Fatalf("secret leaked in env %q", e)
		}
		if e == "PGPASSWORD="+testSecret {
			found = true
		}
	}
	if !found {
		t.Fatal("expected PGPASSWORD in child env")
	}
}

func TestParsePostgresURL_QueryPasswordGoesToEnvNotArgs(t *testing.T) {
	info, err := parsePostgresURL("jdbc:postgresql://localhost:5432/metastore?user=alice&password=" + testSecret)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if info.password != testSecret {
		t.Fatalf("password = %q", info.password)
	}
	args := postgresBaseArgs(info, "postgres")
	assertNoSecretInArgs(t, args)
	env := postgresCmdEnv(nil, info)
	if len(env) != 1 || env[0] != "PGPASSWORD="+testSecret {
		t.Fatalf("env = %#v", env)
	}
}

func TestParseMySQLURL_UserinfoPasswordGoesToEnvNotArgs(t *testing.T) {
	info, err := parseMySQLURL("jdbc:mysql://alice:" + testSecret + "@127.0.0.1:3306/metastore")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	args := append(mysqlBaseArgs(info), "-e", "SELECT 1")
	cmd := exec.Command("mysql", args...)
	cmd.Env = mysqlCmdEnv(nil, info)
	assertNoSecretInArgs(t, cmd.Args)
	found := false
	for _, e := range cmd.Env {
		if e == "MYSQL_PWD="+testSecret {
			found = true
		}
	}
	if !found {
		t.Fatal("expected MYSQL_PWD in child env")
	}
}

func TestParseMySQLURL_QueryPasswordGoesToEnvNotArgs(t *testing.T) {
	info, err := parseMySQLURL("jdbc:mysql://localhost:3306/metastore?user=alice&password=" + testSecret)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	assertNoSecretInArgs(t, mysqlBaseArgs(info))
	env := mysqlCmdEnv(nil, info)
	if len(env) != 1 || env[0] != "MYSQL_PWD="+testSecret {
		t.Fatalf("env = %#v", env)
	}
}

func TestParseJDBCURL_ErrorsRedactPassword(t *testing.T) {
	_, err := parsePostgresURL("jdbc:postgresql://alice:" + testSecret + "@localhost:5432/")
	if err == nil {
		t.Fatal("expected missing database name")
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("error leaked secret: %v", err)
	}

	_, err = parseMySQLURL("jdbc:mysql://alice:" + testSecret + "@localhost:3306/")
	if err == nil {
		t.Fatal("expected missing database name")
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("error leaked secret: %v", err)
	}
}

func TestPostgresCreateArgsOmitPassword(t *testing.T) {
	info, err := parsePostgresURL("jdbc:postgresql://alice:" + testSecret + "@localhost:5432/metastore")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	args := append(postgresBaseArgs(info, "postgres"), "-c", "CREATE DATABASE metastore;")
	cmd := exec.Command("psql", args...)
	cmd.Env = postgresCmdEnv(nil, info)
	assertNoSecretInArgs(t, cmd.Args)
	for _, a := range cmd.Args {
		if strings.Contains(a, "postgresql://") {
			t.Fatalf("postgres command still passes a URL in argv: %q", a)
		}
	}
}
