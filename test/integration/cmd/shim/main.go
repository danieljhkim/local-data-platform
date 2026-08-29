//go:build integration

// Command shim provides deterministic stand-ins for the external programs used
// by the local-data executable. The integration harness installs this one
// binary under several command names and selects behavior from argv[0].
package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type invocation struct {
	PID  int               `json:"pid"`
	Name string            `json:"name"`
	Args []string          `json:"args"`
	Env  map[string]string `json:"env"`
}

type xmlConfiguration struct {
	Properties []struct {
		Name  string `xml:"name"`
		Value string `xml:"value"`
	} `xml:"property"`
}

func main() {
	name := filepath.Base(os.Args[0])
	args := os.Args[1:]
	logInvocation(name, args)

	switch name {
	case "hdfs":
		runHDFS(args)
	case "yarn":
		runYARN(args)
	case "hive":
		runHive(args)
	case "schematool":
		runSchemaTool(args)
	case "psql":
		fmt.Println("1")
	case "mysql":
		fmt.Println("metastore")
	case "jps":
		return
	case "pgrep":
		os.Exit(1)
	case "lsof":
		runLSOF(args)
	case "ps":
		runPS(args)
	case "unrelated":
		waitForSignal(nil)
	case "beeline", "hadoop", "pyspark", "spark-submit", "env-probe":
		return
	default:
		fmt.Fprintf(os.Stderr, "unsupported shim name %q\n", name)
		os.Exit(64)
	}
}

func runHDFS(args []string) {
	if len(args) == 0 {
		return
	}
	if args[0] == "namenode" && contains(args, "-format") {
		versionDir := filepath.Join(baseDir(), "state", "hdfs", "namenode", "current")
		if err := os.MkdirAll(versionDir, 0o755); err != nil {
			fatal(err)
		}
		if err := os.WriteFile(filepath.Join(versionDir, "VERSION"), []byte("clusterID=integration\n"), 0o644); err != nil {
			fatal(err)
		}
		return
	}
	if args[0] == "namenode" || args[0] == "datanode" {
		if controlledFailure("hdfs-" + args[0]) {
			os.Exit(42)
		}
		waitForSignal(nil)
		return
	}
	if len(args) >= 3 && args[0] == "dfsadmin" && args[1] == "-safemode" && args[2] == "get" {
		if controlExists("hdfs-safemode-timeout") {
			fmt.Println("Safe mode is ON")
		} else {
			fmt.Println("Safe mode is OFF")
		}
	}
}

func runYARN(args []string) {
	if len(args) == 0 {
		return
	}
	if args[0] == "resourcemanager" || args[0] == "nodemanager" {
		if controlledFailure("yarn-" + args[0]) {
			os.Exit(42)
		}
		waitForSignal(nil)
	}
}

func runHive(args []string) {
	if len(args) < 2 || args[0] != "--service" {
		return
	}
	service := args[1]
	if controlledFailure("hive-" + service) {
		os.Exit(42)
	}

	var listener net.Listener
	if !controlExists("hive-no-listen") {
		port := hivePort(service)
		if port > 0 {
			var err error
			listener, err = net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
			if err != nil {
				fatal(fmt.Errorf("listen on %d: %w", port, err))
			}
			go acceptConnections(listener)
		}
	}
	waitForSignal(listener)
}

func runSchemaTool(args []string) {
	if contains(args, "-initSchema") {
		fmt.Println("Initialization script completed")
		return
	}
	fmt.Println("Hive distribution version 4.0.0")
}

func runLSOF(args []string) {
	port := 0
	for _, arg := range args {
		if strings.HasPrefix(arg, "-iTCP:") {
			port, _ = strconv.Atoi(strings.TrimPrefix(arg, "-iTCP:"))
		}
	}
	service := ""
	if port == hivePort("metastore") {
		service = "metastore"
	} else if port == hivePort("hiveserver2") {
		service = "hiveserver2"
	}
	if service == "" {
		os.Exit(1)
	}
	data, err := os.ReadFile(filepath.Join(baseDir(), "state", "hive", "pids", service+".pid"))
	if err != nil {
		os.Exit(1)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || !processAlive(pid) {
		os.Exit(1)
	}
	fmt.Println("COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME")
	fmt.Printf("hive %d integration 1u IPv4 0t0 TCP 127.0.0.1:%d (LISTEN)\n", pid, port)
}

func runPS(args []string) {
	pid := 0
	for index, arg := range args {
		if arg == "-p" && index+1 < len(args) {
			pid, _ = strconv.Atoi(args[index+1])
		}
	}
	file, err := os.Open(filepath.Join(baseDir(), "shim.log"))
	if err != nil {
		os.Exit(1)
	}
	defer file.Close()
	var matched *invocation
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record invocation
		if json.Unmarshal(scanner.Bytes(), &record) == nil && record.PID == pid && record.Name != "ps" {
			copy := record
			matched = &copy
		}
	}
	if matched == nil {
		os.Exit(1)
	}
	parts := append([]string{matched.Name}, matched.Args...)
	fmt.Print(strings.Join(parts, " "))
	for key, value := range matched.Env {
		if value != "" {
			fmt.Printf(" %s=%s", key, value)
		}
	}
	fmt.Println()
}

func hivePort(service string) int {
	confDir := os.Getenv("HIVE_CONF_DIR")
	if confDir == "" {
		confDir = filepath.Join(baseDir(), "conf", "current", "hive")
	}
	data, err := os.ReadFile(filepath.Join(confDir, "hive-site.xml"))
	if err != nil {
		return 0
	}
	var cfg xmlConfiguration
	if err := xml.Unmarshal(data, &cfg); err != nil {
		return 0
	}
	key := "hive.server2.thrift.port"
	if service == "metastore" {
		key = "hive.metastore.uris"
	}
	value := ""
	for _, property := range cfg.Properties {
		if property.Name == key {
			value = strings.TrimSpace(property.Value)
		}
	}
	if service == "metastore" {
		if index := strings.LastIndex(value, ":"); index >= 0 {
			value = value[index+1:]
		}
	}
	port, _ := strconv.Atoi(value)
	return port
}

func acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}
}

func waitForSignal(listener net.Listener) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	if listener != nil {
		_ = listener.Close()
	}
}

func controlledFailure(key string) bool {
	return controlExists("fail-" + key)
}

func controlExists(name string) bool {
	_, err := os.Stat(filepath.Join(baseDir(), "shim-control", name))
	return err == nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func baseDir() string {
	if value := os.Getenv("BASE_DIR"); value != "" {
		return value
	}
	if confDir := os.Getenv("HADOOP_CONF_DIR"); confDir != "" {
		return filepath.Clean(filepath.Join(confDir, "..", "..", ".."))
	}
	return filepath.Join(os.Getenv("HOME"), "local-data-platform")
}

func logInvocation(name string, args []string) {
	dir := baseDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fatal(err)
	}
	record := invocation{
		PID:  os.Getpid(),
		Name: name,
		Args: append([]string(nil), args...),
		Env: map[string]string{
			"ACTIVE_PROFILE":  os.Getenv("ACTIVE_PROFILE"),
			"BASE_DIR":        os.Getenv("BASE_DIR"),
			"HADOOP_CONF_DIR": os.Getenv("HADOOP_CONF_DIR"),
			"HIVE_CONF_DIR":   os.Getenv("HIVE_CONF_DIR"),
			"SPARK_CONF_DIR":  os.Getenv("SPARK_CONF_DIR"),
			"TERM":            os.Getenv("TERM"),
		},
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		fatal(err)
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(filepath.Join(dir, "shim.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fatal(err)
	}
	_, writeErr := file.Write(encoded)
	closeErr := file.Close()
	if writeErr != nil {
		fatal(writeErr)
	}
	if closeErr != nil {
		fatal(closeErr)
	}
}

func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
