//go:build integration

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	repositoryRoot string
	localDataBin   string
	commandShimBin string
)

type invocation struct {
	PID  int               `json:"pid"`
	Name string            `json:"name"`
	Args []string          `json:"args"`
	Env  map[string]string `json:"env"`
}

type sandbox struct {
	t               *testing.T
	baseDir         string
	toolBin         string
	metastorePort   int
	hiveServer2Port int
	env             []string
}

type xmlConfiguration struct {
	XMLName    xml.Name      `xml:"configuration"`
	Properties []xmlProperty `xml:"property"`
}

type xmlProperty struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

func TestMain(m *testing.M) {
	var err error
	repositoryRoot, err = filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	buildDir, err := os.MkdirTemp("", "local-data-integration-build-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	localDataBin = filepath.Join(buildDir, "local-data")
	commandShimBin = filepath.Join(buildDir, "command-shim")
	if err := buildBinary(localDataBin, "./cmd/local-data"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := buildBinary(commandShimBin, "-tags=integration", "./test/integration/cmd/shim"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	code := m.Run()
	if err := os.RemoveAll(buildDir); err != nil {
		fmt.Fprintf(os.Stderr, "remove integration build directory: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func buildBinary(destination string, args ...string) error {
	buildArgs := append([]string{"build", "-o", destination}, args...)
	cmd := exec.Command("go", buildArgs...)
	cmd.Dir = repositoryRoot
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go %s: %w\n%s", strings.Join(buildArgs, " "), err, output)
	}
	return nil
}

func TestHermeticLifecycleAndWrappers(t *testing.T) {
	s := newSandbox(t)
	s.initialize(true)
	s.mustRun(nil, "profile", "set", "hdfs")

	startOutput := s.mustRun(nil, "start")
	for _, phrase := range []string{"start hdfs", "start yarn", "start hive", "HiveServer2 is ready"} {
		if !strings.Contains(startOutput, phrase) {
			t.Fatalf("start output missing %q:\n%s", phrase, startOutput)
		}
	}

	statusOutput := s.mustRun(nil, "status")
	for _, phrase := range []string{"namenode", "datanode", "resourcemanager", "nodemanager", "metastore", "hiveserver2", "running", "listening"} {
		if !strings.Contains(statusOutput, phrase) {
			t.Fatalf("status output missing %q:\n%s", phrase, statusOutput)
		}
	}

	s.mustRun(nil, "env", "exec", "--", "env-probe", "alpha", "two words")
	s.mustRun(nil, "hdfs", "dfs", "-ls", "/")
	s.mustRun(nil, "yarn", "node", "-list")
	s.mustRun(nil, "hive", "-e", "SELECT 1")
	s.mustRun(nil, "hadoop", "version")
	s.mustRun(nil, "pyspark", "--conf", "spark.sql.shuffle.partitions=2")
	s.mustRun(nil, "spark-submit", "--class", "example.Main", "job.jar")

	records := s.records()
	for _, name := range []string{"hdfs", "yarn", "hive", "jps", "lsof", "schematool", "psql"} {
		if !hasInvocation(records, name) {
			t.Errorf("expected %s shim to be exercised; records: %#v", name, records)
		}
	}
	assertArgs(t, records, "env-probe", []string{"alpha", "two words"})
	assertArgs(t, records, "hdfs", []string{"dfs", "-ls", "/"})
	assertArgs(t, records, "yarn", []string{"node", "-list"})
	assertArgs(t, records, "beeline", []string{"-u", "jdbc:hive2://localhost:10000", "-e", "SELECT 1"})
	assertArgs(t, records, "spark-submit", []string{"--class", "example.Main", "job.jar"})

	expectedHadoopConf := filepath.Join(s.baseDir, "conf", "current", "hadoop")
	for _, record := range records {
		if record.Name == "hdfs" && record.Env["HADOOP_CONF_DIR"] != expectedHadoopConf {
			t.Errorf("hdfs %v received HADOOP_CONF_DIR=%q, want %q", record.Args, record.Env["HADOOP_CONF_DIR"], expectedHadoopConf)
		}
	}
	for _, record := range records {
		if record.Name == "beeline" && record.Env["TERM"] != "dumb" {
			t.Errorf("beeline TERM=%q, want dumb", record.Env["TERM"])
		}
	}

	daemonPIDs := daemonPIDs(records)
	s.mustRun(nil, "stop")
	for _, pid := range daemonPIDs {
		waitForPIDExit(t, pid, 5*time.Second)
	}
}

func TestDownstreamStartupFailureStopsDispatch(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "hdfs")
	s.setControl("fail-yarn-nodemanager")

	output, err := s.run(nil, "start")
	if err == nil {
		t.Fatalf("start unexpectedly succeeded:\n%s\nrecords: %#v", output, s.records())
	}
	if !strings.Contains(output, "failed to start NodeManager") {
		t.Fatalf("start failure missing NodeManager context:\n%s", output)
	}
	records := s.records()
	if hasInvocationWithArgs(records, "hive", []string{"--service"}) {
		t.Fatalf("Hive was dispatched after the YARN failure: %#v", records)
	}

	daemonPIDs := daemonPIDs(records)
	s.mustRun(nil, "stop")
	for _, pid := range daemonPIDs {
		waitForPIDExit(t, pid, 5*time.Second)
	}
}

func TestHDFSReadinessTimeoutUsesActiveOverlay(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "hdfs")
	s.setControl("hdfs-safemode-timeout")

	output := s.mustRun(nil, "start", "hdfs")
	if !strings.Contains(output, "HDFS did not exit safe mode after 10 retries") {
		t.Fatalf("readiness timeout was not reported:\n%s", output)
	}

	expected := filepath.Join(s.baseDir, "conf", "current", "hadoop")
	foundProbe := false
	for _, record := range s.records() {
		if record.Name == "hdfs" && equalArgs(record.Args, []string{"dfsadmin", "-safemode", "get"}) {
			foundProbe = true
			if record.Env["HADOOP_CONF_DIR"] != expected {
				t.Fatalf("safe-mode probe HADOOP_CONF_DIR=%q, want %q", record.Env["HADOOP_CONF_DIR"], expected)
			}
		}
	}
	if !foundProbe {
		t.Fatal("safe-mode readiness probe was not executed")
	}
	s.mustRun(nil, "stop", "hdfs")
}

func TestStalePIDOwnershipMismatchDoesNotKillUnrelatedProcess(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "local")

	cmd := exec.Command(filepath.Join(s.toolBin, "unrelated"), "sentinel")
	cmd.Env = s.env
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	finished := false
	t.Cleanup(func() {
		if finished {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})

	pidDir := filepath.Join(s.baseDir, "state", "hive", "pids")
	if err := os.MkdirAll(pidDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(pidDir, "metastore.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		t.Fatal(err)
	}

	output := s.mustRun(nil, "stop", "hive")
	if !strings.Contains(output, "refusing to stop Hive metastore") || !strings.Contains(output, "does not match expected service") {
		t.Fatalf("ownership mismatch was not reported:\n%s", output)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process was signaled by stop: %v", err)
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("stale PID file still exists after guarded stop: %v", err)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		finished = true
	case <-time.After(2 * time.Second):
		t.Fatal("test-owned unrelated process did not exit during test cleanup")
	}
}

func newSandbox(t *testing.T) *sandbox {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "home")
	baseDir := filepath.Join(home, "local-data-platform")
	toolHome := filepath.Join(root, "tool-home")
	toolBin := filepath.Join(toolHome, "bin")
	for _, dir := range []string{home, baseDir, toolBin, filepath.Join(toolHome, "lib")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(toolHome, "lib", "postgresql-integration.jar"), []byte("shim"), 0o644); err != nil {
		t.Fatal(err)
	}

	commands := []string{"hdfs", "yarn", "hive", "jps", "pgrep", "lsof", "ps", "schematool", "psql", "mysql", "beeline", "hadoop", "pyspark", "spark-submit", "env-probe", "unrelated"}
	for _, name := range commands {
		if err := os.Symlink(commandShimBin, filepath.Join(toolBin, name)); err != nil {
			t.Fatal(err)
		}
	}

	s := &sandbox{t: t, baseDir: baseDir, toolBin: toolBin, metastorePort: freePort(t), hiveServer2Port: freePort(t)}
	s.env = isolatedEnv(map[string]string{
		"HOME":        home,
		"USER":        "integration",
		"LOGNAME":     "integration",
		"NO_COLOR":    "1",
		"HADOOP_HOME": toolHome,
		"HIVE_HOME":   toolHome,
		"SPARK_HOME":  toolHome,
		"PATH":        toolBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"LC_ALL":      "C",
		"LANG":        "C",
	})
	s.writeOverrides(s.metastorePort, s.hiveServer2Port)
	t.Cleanup(s.cleanup)
	return s
}

func isolatedEnv(overrides map[string]string) []string {
	removed := map[string]bool{
		"ACTIVE_PROFILE": true, "BASE_DIR": true, "HADOOP_CONF_DIR": true,
		"HADOOP_HOME": true, "HIVE_CONF_DIR": true, "HIVE_HOME": true,
		"HOME": true, "JAVA_HOME": true, "PATH": true, "SPARK_CONF_DIR": true,
		"SPARK_HOME": true, "TERM": true, "USER": true, "LOGNAME": true,
		"NO_COLOR": true, "LC_ALL": true, "LANG": true,
	}
	result := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if !removed[key] {
			result = append(result, entry)
		}
	}
	for key, value := range overrides {
		result = append(result, key+"="+value)
	}
	return result
}

func (s *sandbox) writeOverrides(metastorePort, hiveServerPort int) {
	s.t.Helper()
	confDir := filepath.Join(s.baseDir, "conf")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		s.t.Fatal(err)
	}
	contents := fmt.Sprintf(`profiles:
  local:
    hive:
      hive.metastore.uris: thrift://127.0.0.1:%d
      hive.server2.thrift.port: %d
  hdfs:
    hive:
      hive.metastore.uris: thrift://127.0.0.1:%d
      hive.server2.thrift.port: %d
`, metastorePort, hiveServerPort, metastorePort, hiveServerPort)
	if err := os.WriteFile(filepath.Join(confDir, "overrides.yaml"), []byte(contents), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sandbox) initialize(postgres bool) {
	s.t.Helper()
	args := []string{"init", "--user", "integration"}
	if postgres {
		args = append(args, "--db-type", "postgres", "--db-url", "jdbc:postgresql://localhost:5432/metastore")
	} else {
		args = append(args, "--db-type", "derby")
	}
	s.mustRun(strings.NewReader("\n\n\n\n"), args...)
	for _, profile := range []string{"local", "hdfs"} {
		s.setGeneratedHivePorts(profile)
	}
}

func (s *sandbox) setGeneratedHivePorts(profile string) {
	s.t.Helper()
	path := filepath.Join(s.baseDir, "conf", "profiles", profile, "hive", "hive-site.xml")
	data, err := os.ReadFile(path)
	if err != nil {
		s.t.Fatal(err)
	}
	var cfg xmlConfiguration
	if err := xml.Unmarshal(data, &cfg); err != nil {
		s.t.Fatal(err)
	}
	for index := range cfg.Properties {
		switch cfg.Properties[index].Name {
		case "hive.metastore.uris":
			cfg.Properties[index].Value = fmt.Sprintf("thrift://127.0.0.1:%d", s.metastorePort)
		case "hive.server2.thrift.port":
			cfg.Properties[index].Value = strconv.Itoa(s.hiveServer2Port)
		}
	}
	encoded, err := xml.MarshalIndent(cfg, "", "  ")
	if err != nil {
		s.t.Fatal(err)
	}
	encoded = append([]byte(xml.Header), append(encoded, '\n')...)
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sandbox) setControl(name string) {
	s.t.Helper()
	dir := filepath.Join(s.baseDir, "shim-control")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte("enabled\n"), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sandbox) mustRun(input *strings.Reader, args ...string) string {
	s.t.Helper()
	var reader *strings.Reader
	if input != nil {
		reader = input
	}
	output, err := s.run(reader, args...)
	if err != nil {
		s.t.Fatalf("local-data %s failed: %v\n%s", strings.Join(args, " "), err, output)
	}
	return output
}

func (s *sandbox) run(input *strings.Reader, args ...string) (string, error) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, localDataBin, args...)
	cmd.Dir = repositoryRoot
	cmd.Env = s.env
	if input != nil {
		cmd.Stdin = input
	}
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(output), fmt.Errorf("command timeout: %w", ctx.Err())
	}
	return string(output), err
}

func (s *sandbox) records() []invocation {
	s.t.Helper()
	file, err := os.Open(filepath.Join(s.baseDir, "shim.log"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		s.t.Fatal(err)
	}
	defer file.Close()

	var records []invocation
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record invocation
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			s.t.Fatalf("decode shim record %q: %v", scanner.Text(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		s.t.Fatal(err)
	}
	return records
}

func (s *sandbox) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, localDataBin, "stop")
	cmd.Dir = repositoryRoot
	cmd.Env = s.env
	_ = cmd.Run()
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func hasInvocation(records []invocation, name string) bool {
	for _, record := range records {
		if record.Name == name {
			return true
		}
	}
	return false
}

func hasInvocationWithArgs(records []invocation, name string, prefix []string) bool {
	for _, record := range records {
		if record.Name == name && argsHavePrefix(record.Args, prefix) {
			return true
		}
	}
	return false
}

func assertArgs(t *testing.T, records []invocation, name string, args []string) {
	t.Helper()
	for _, record := range records {
		if record.Name == name && equalArgs(record.Args, args) {
			return
		}
	}
	t.Errorf("missing invocation %s %q", name, args)
}

func equalArgs(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range actual {
		if actual[index] != expected[index] {
			return false
		}
	}
	return true
}

func argsHavePrefix(actual, prefix []string) bool {
	return len(actual) >= len(prefix) && equalArgs(actual[:len(prefix)], prefix)
}

func daemonPIDs(records []invocation) []int {
	seen := make(map[int]bool)
	var result []int
	for _, record := range records {
		daemon := record.Name == "hdfs" && len(record.Args) == 1 && (record.Args[0] == "namenode" || record.Args[0] == "datanode")
		daemon = daemon || record.Name == "yarn" && len(record.Args) == 1 && (record.Args[0] == "resourcemanager" || record.Args[0] == "nodemanager")
		daemon = daemon || record.Name == "hive" && argsHavePrefix(record.Args, []string{"--service"})
		if daemon && !seen[record.PID] {
			seen[record.PID] = true
			result = append(result, record.PID)
		}
	}
	return result
}

func waitForPIDExit(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		process, err := os.FindProcess(pid)
		if err != nil || process.Signal(syscall.Signal(0)) != nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Errorf("pid %d still exists after %s", pid, timeout)
}
