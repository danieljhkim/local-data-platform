//go:build integration

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
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
	configuredHiveURL := fmt.Sprintf("jdbc:hive2://localhost:%d", s.hiveServer2Port)
	assertArgs(t, records, "beeline", []string{"-u", configuredHiveURL, "-e", "SELECT 1"})

	explicitHiveURL := fmt.Sprintf("jdbc:hive2://127.0.0.1:%d", s.hiveServer2Port)
	s.mustRun(nil, "hive", "-u", explicitHiveURL, "-e", "SELECT 1")
	records = s.records()
	assertArgs(t, records, "beeline", []string{"-u", explicitHiveURL, "-e", "SELECT 1"})

	wrongHiveURL := fmt.Sprintf("jdbc:hive2://127.0.0.1:%d", freePort(t))
	if output, err := s.run(nil, "hive", "-u", wrongHiveURL, "-e", "SELECT 1"); err == nil {
		t.Fatalf("hive with wrong endpoint unexpectedly succeeded:\n%s", output)
	}
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

func TestStatusAndStopUseRuntimeStateWhenStartupConfigDisappears(t *testing.T) {
	s := newSandbox(t)
	s.initialize(true)
	s.mustRun(nil, "profile", "set", "hdfs")
	s.mustRun(nil, "start")
	daemons := daemonPIDs(s.records())
	if len(daemons) == 0 {
		t.Fatal("start did not record any sandbox-owned daemon PIDs")
	}

	if err := os.RemoveAll(filepath.Join(s.baseDir, "conf", "profiles")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(s.baseDir, "conf", "current", "hive", "hive-site.xml")); err != nil {
		t.Fatal(err)
	}
	s.env = withoutEnv(s.env, "HIVE_HOME")

	statusOutput, statusErr := s.run(nil, "status")
	if statusErr == nil {
		t.Fatalf("status unexpectedly reported complete observation without listener settings:\n%s", statusOutput)
	}
	for _, process := range []string{"namenode", "datanode", "resourcemanager", "nodemanager", "metastore", "hiveserver2"} {
		if !strings.Contains(statusOutput, process) {
			t.Fatalf("status lost %s process observation after startup config disappeared:\n%s", process, statusOutput)
		}
	}
	if _, err := os.Stat(filepath.Join(s.baseDir, "conf", "current", "hive", "hive-site.xml")); !os.IsNotExist(err) {
		t.Fatalf("status republished removed Hive configuration: %v", err)
	}

	if output, err := s.run(nil, "stop"); err != nil {
		t.Fatalf("stop failed after startup configuration disappeared: %v\n%s", err, output)
	}
	for _, pid := range daemons {
		waitForPIDExit(t, pid, 5*time.Second)
	}
	if _, err := os.Stat(filepath.Join(s.baseDir, "conf", "current", "hive", "hive-site.xml")); !os.IsNotExist(err) {
		t.Fatalf("stop republished removed Hive configuration: %v", err)
	}
}

func TestEnvExecAndWrapperPreserveChildExitCodesAndStreams(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "local")

	commands := []struct {
		name string
		args func(int) []string
	}{
		{name: "env exec", args: func(code int) []string {
			return []string{"env", "exec", "--", "env-probe", "exit-probe", strconv.Itoa(code)}
		}},
		{name: "hadoop wrapper", args: func(code int) []string {
			return []string{"hadoop", "exit-probe", strconv.Itoa(code)}
		}},
	}

	for _, command := range commands {
		for _, want := range []int{0, 2, 42} {
			t.Run(fmt.Sprintf("%s/status-%d", command.name, want), func(t *testing.T) {
				output, err := s.run(nil, command.args(want)...)
				if got := childExitCode(err); got != want {
					t.Fatalf("exit code = %d, want %d; output:\n%s", got, want, output)
				}
				if !strings.Contains(output, "stdout") || !strings.Contains(output, "stderr") {
					t.Fatalf("child streams missing from output:\n%s", output)
				}
			})
		}
	}
}

func TestNativeValidationAndLaunchErrorsRemainReadable(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "local")

	tests := []struct {
		name       string
		args       []string
		wantOutput string
	}{
		{
			name:       "missing command argument",
			args:       []string{"env", "exec"},
			wantOutput: "usage: local-data env exec -- <cmd...>",
		},
		{
			name:       "missing executable",
			args:       []string{"env", "exec", "--", "does-not-exist"},
			wantOutput: "executable file not found",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output, err := s.run(nil, test.args...)
			if got := childExitCode(err); got == 0 {
				t.Fatalf("native error unexpectedly succeeded; output:\n%s", output)
			}
			if !strings.Contains(output, test.wantOutput) {
				t.Fatalf("output missing %q:\n%s", test.wantOutput, output)
			}
		})
	}
}

func childExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
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

	for _, pid := range daemonPIDs(records) {
		waitForPIDExit(t, pid, 5*time.Second)
	}
	assertNoPIDFiles(t, s.baseDir)
}

func TestHDFSReadinessTimeoutUsesActiveOverlay(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "hdfs")
	s.setControl("hdfs-safemode-timeout")

	output, err := s.run(nil, "start", "hdfs")
	if err == nil {
		t.Fatalf("HDFS start unexpectedly succeeded after safe-mode timeout:\n%s", output)
	}
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
	for _, pid := range daemonPIDs(s.records()) {
		waitForPIDExit(t, pid, 5*time.Second)
	}
	assertNoPIDFiles(t, s.baseDir)
}

// TestInterruptDuringStartRollsBackDetachedServices sends SIGINT to the
// process group of a start that is waiting on readiness, as a terminal does for
// Ctrl-C. The daemon must not see that SIGINT; the CLI must roll it back with
// SIGTERM and exit 130.
func TestInterruptDuringStartRollsBackDetachedServices(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "local")
	s.setControl("hive-no-listen")

	start := s.startInOwnProcessGroup("start")
	metastorePID := s.waitForSignalReadyDaemon("hive", []string{"--service", "metastore"}, 15*time.Second)
	if err := syscall.Kill(-start.pgid, syscall.SIGINT); err != nil {
		t.Fatalf("send SIGINT to start's process group: %v", err)
	}

	output, err := start.wait(30 * time.Second)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 130 {
		t.Fatalf("start after SIGINT: %v, want exit status 130\n%s", err, output)
	}
	if !strings.Contains(output, "interrupted by SIGINT") {
		t.Fatalf("start output does not report the interrupt:\n%s", output)
	}

	waitForPIDExit(t, metastorePID, 5*time.Second)
	if events := s.signalEvents(metastorePID); !equalArgs(events, []string{"ready", "SIGTERM"}) {
		t.Fatalf("metastore pid %d signal events = %q, want only the rollback SIGTERM", metastorePID, events)
	}
	if hasInvocationWithArgs(s.records(), "hive", []string{"--service", "hiveserver2"}) {
		t.Fatal("HiveServer2 was dispatched after the interrupt")
	}
	assertNoPIDFiles(t, s.baseDir)
}

// TestStartedServicesLeaveCallerProcessGroup covers a script that runs
// `local-data start` and keeps going: Ctrl-C there signals the script's whole
// process group, which must no longer contain the daemons.
func TestStartedServicesLeaveCallerProcessGroup(t *testing.T) {
	s := newSandbox(t)
	s.initialize(false)
	s.mustRun(nil, "profile", "set", "local")

	start := s.startInOwnProcessGroup("start")
	if output, err := start.wait(30 * time.Second); err != nil {
		t.Fatalf("start failed: %v\n%s", err, output)
	}
	daemons := daemonPIDs(s.records())
	if len(daemons) != 2 {
		t.Fatalf("daemon pids = %v, want metastore and HiveServer2", daemons)
	}

	if err := syscall.Kill(-start.pgid, syscall.SIGINT); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("SIGINT to the caller's process group = %v, want ESRCH (daemons must not be members)", err)
	}
	for _, pid := range daemons {
		if events := s.signalEvents(pid); !equalArgs(events, []string{"ready"}) {
			t.Fatalf("daemon pid %d signal events = %q, want none", pid, events)
		}
	}
	if status := s.mustRun(nil, "status"); strings.Contains(status, "stopped") {
		t.Fatalf("services stopped after caller group SIGINT:\n%s", status)
	}
}

// TestSignalDuringBlockingStartupSubprocess sends one signal directly to the
// start CLI (not its process group, as a service manager or script would) while
// a startup subprocess hangs. The subprocess and its descendant must be
// canceled without a second signal, only services this invocation launched may
// be rolled back, in reverse order, and nothing later may be dispatched.
func TestSignalDuringBlockingStartupSubprocess(t *testing.T) {
	cases := []struct {
		name string
		// preStartHDFS starts HDFS in a prior invocation so the hung run finds
		// it already running and must leave it alone.
		preStartHDFS bool
		controls     []string
		hang         string
		hangArgs     []string
		signal       syscall.Signal
		wantExit     int
		wantSignal   string
		// rolledBack lists the daemons this invocation launches before the hang.
		rolledBack []invocationKey
		// notDispatched lists daemons that must never be launched.
		notDispatched []invocationKey
		// survivors lists daemons from the prior invocation.
		survivors []invocationKey
	}{
		{
			name:          "schema info SIGTERM",
			preStartHDFS:  true,
			controls:      []string{"schematool-info-hang"},
			hang:          "schematool",
			hangArgs:      []string{"-dbType", "postgres", "-info"},
			signal:        syscall.SIGTERM,
			wantExit:      143,
			wantSignal:    "SIGTERM",
			rolledBack:    []invocationKey{{"yarn", "nodemanager"}, {"yarn", "resourcemanager"}},
			notDispatched: []invocationKey{{"hive", "--service"}},
			survivors:     []invocationKey{{"hdfs", "datanode"}, {"hdfs", "namenode"}},
		},
		{
			name:          "schema init SIGINT",
			preStartHDFS:  true,
			controls:      []string{"schematool-not-initialized", "schematool-init-hang"},
			hang:          "schematool",
			hangArgs:      []string{"-dbType", "postgres", "-initSchema"},
			signal:        syscall.SIGINT,
			wantExit:      130,
			wantSignal:    "SIGINT",
			rolledBack:    []invocationKey{{"yarn", "nodemanager"}, {"yarn", "resourcemanager"}},
			notDispatched: []invocationKey{{"hive", "--service"}},
			survivors:     []invocationKey{{"hdfs", "datanode"}, {"hdfs", "namenode"}},
		},
		{
			name:          "namenode format SIGTERM",
			controls:      []string{"hdfs-format-hang"},
			hang:          "hdfs",
			hangArgs:      []string{"namenode", "-format", "-force", "-nonInteractive"},
			signal:        syscall.SIGTERM,
			wantExit:      143,
			wantSignal:    "SIGTERM",
			notDispatched: []invocationKey{{"hdfs", "namenode"}, {"hdfs", "datanode"}, {"yarn", "resourcemanager"}, {"hive", "--service"}},
		},
		{
			name:          "namenode format SIGINT",
			controls:      []string{"hdfs-format-hang"},
			hang:          "hdfs",
			hangArgs:      []string{"namenode", "-format", "-force", "-nonInteractive"},
			signal:        syscall.SIGINT,
			wantExit:      130,
			wantSignal:    "SIGINT",
			notDispatched: []invocationKey{{"hdfs", "namenode"}, {"hdfs", "datanode"}, {"yarn", "resourcemanager"}, {"hive", "--service"}},
		},
		{
			name:          "hdfs control SIGINT",
			controls:      []string{"hdfs-mkdir-hang"},
			hang:          "hdfs",
			hangArgs:      []string{"dfs", "-mkdir", "-p", "/tmp"},
			signal:        syscall.SIGINT,
			wantExit:      130,
			wantSignal:    "SIGINT",
			rolledBack:    []invocationKey{{"hdfs", "datanode"}, {"hdfs", "namenode"}},
			notDispatched: []invocationKey{{"yarn", "resourcemanager"}, {"hive", "--service"}},
		},
		{
			name:          "hdfs control SIGTERM",
			controls:      []string{"hdfs-mkdir-hang"},
			hang:          "hdfs",
			hangArgs:      []string{"dfs", "-mkdir", "-p", "/tmp"},
			signal:        syscall.SIGTERM,
			wantExit:      143,
			wantSignal:    "SIGTERM",
			rolledBack:    []invocationKey{{"hdfs", "datanode"}, {"hdfs", "namenode"}},
			notDispatched: []invocationKey{{"yarn", "resourcemanager"}, {"hive", "--service"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSandbox(t)
			s.initialize(true)
			s.mustRun(nil, "profile", "set", "hdfs")

			var survivorPIDs []int
			if tc.preStartHDFS {
				s.mustRun(nil, "start", "hdfs")
				for _, key := range tc.survivors {
					survivorPIDs = append(survivorPIDs, s.daemonPID(key))
				}
			}
			before := len(s.records())
			for _, control := range tc.controls {
				s.setControl(control)
			}

			start := s.startInOwnProcessGroup("start")
			hangPID := s.waitForSignalReadyDaemon(tc.hang, tc.hangArgs, 15*time.Second)
			childPID := s.signalChildPID(hangPID)
			rolledBackPIDs := make([]int, 0, len(tc.rolledBack))
			for _, key := range tc.rolledBack {
				rolledBackPIDs = append(rolledBackPIDs, s.waitForDaemonPID(key, before, 15*time.Second))
			}

			if err := syscall.Kill(start.cmd.Process.Pid, tc.signal); err != nil {
				t.Fatalf("send %v to start: %v", tc.signal, err)
			}
			output, err := start.wait(15 * time.Second)
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != tc.wantExit {
				t.Fatalf("start after %v: %v, want exit status %d\n%s", tc.signal, err, tc.wantExit, output)
			}
			if !strings.Contains(output, "interrupted by "+tc.wantSignal) {
				t.Fatalf("start output does not report the interrupt:\n%s", output)
			}

			// The hung subprocess and its descendant were canceled by the CLI.
			waitForPIDExit(t, hangPID, 5*time.Second)
			waitForPIDExit(t, childPID, 5*time.Second)
			if events := s.signalEvents(hangPID); !hasEvent(events, "SIGTERM") {
				t.Fatalf("hung subprocess pid %d signal events = %q, want the CLI's SIGTERM", hangPID, events)
			}

			// Only this invocation's daemons were rolled back, newest first.
			for _, pid := range rolledBackPIDs {
				waitForPIDExit(t, pid, 5*time.Second)
				if events := s.signalEvents(pid); !equalArgs(events, []string{"ready", "SIGTERM"}) {
					t.Fatalf("rolled back pid %d signal events = %q, want the rollback SIGTERM only", pid, events)
				}
			}
			if order := s.sigtermOrder(rolledBackPIDs); !equalInts(order, rolledBackPIDs) {
				t.Fatalf("rollback SIGTERM order = %v, want reverse start order %v", order, rolledBackPIDs)
			}
			for _, key := range tc.notDispatched {
				for _, record := range s.records()[before:] {
					if key.matches(record) {
						t.Fatalf("%s %s was dispatched after the interrupt", key.name, key.arg)
					}
				}
			}
			for _, pid := range survivorPIDs {
				if err := syscall.Kill(pid, 0); err != nil {
					t.Fatalf("pre-existing daemon pid %d did not survive rollback: %v", pid, err)
				}
				if events := s.signalEvents(pid); !equalArgs(events, []string{"ready"}) {
					t.Fatalf("pre-existing daemon pid %d signal events = %q, want none", pid, events)
				}
			}
			if !tc.preStartHDFS {
				assertNoPIDFiles(t, s.baseDir)
			}
			if tc.hang == "hdfs" && tc.hangArgs[0] == "namenode" {
				version := filepath.Join(s.baseDir, "state", "hdfs", "namenode", "current", "VERSION")
				if _, err := os.Stat(version); err == nil {
					t.Fatalf("canceled format left a VERSION file: %s", version)
				}
			}
		})
	}
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

	output, err := s.run(nil, "stop", "hive")
	if err == nil {
		t.Fatalf("stop hive unexpectedly succeeded after ownership mismatch:\n%s", output)
	}
	if !strings.Contains(output, "refusing to stop Hive metastore") || !strings.Contains(output, "does not match expected service") {
		t.Fatalf("ownership mismatch was not reported:\n%s", output)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process was signaled by stop: %v", err)
	}
	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("PID ownership record was removed after guarded stop: %v", err)
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

func withoutEnv(environment []string, key string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
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
	s.mustRun(strings.NewReader("\n\n\nempty\n"), args...)
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

// backgroundRun is a local-data invocation in its own process group, which
// stands in for a terminal's foreground job.
type backgroundRun struct {
	t        *testing.T
	cmd      *exec.Cmd
	pgid     int
	output   bytes.Buffer
	err      error
	finished chan struct{}
}

func (s *sandbox) startInOwnProcessGroup(args ...string) *backgroundRun {
	s.t.Helper()
	run := &backgroundRun{t: s.t, finished: make(chan struct{})}
	run.cmd = exec.Command(localDataBin, args...)
	run.cmd.Dir = repositoryRoot
	run.cmd.Env = s.env
	run.cmd.Stdout = &run.output
	run.cmd.Stderr = &run.output
	run.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := run.cmd.Start(); err != nil {
		s.t.Fatalf("start local-data %s: %v", strings.Join(args, " "), err)
	}
	run.pgid = run.cmd.Process.Pid
	go func() {
		run.err = run.cmd.Wait()
		close(run.finished)
	}()
	s.t.Cleanup(func() {
		select {
		case <-run.finished:
		default:
			_ = syscall.Kill(-run.pgid, syscall.SIGKILL)
			<-run.finished
		}
	})
	return run
}

// wait returns the combined output and exit error once the run finishes.
func (r *backgroundRun) wait(timeout time.Duration) (string, error) {
	r.t.Helper()
	select {
	case <-r.finished:
		return r.output.String(), r.err
	case <-time.After(timeout):
		_ = syscall.Kill(-r.pgid, syscall.SIGKILL)
		<-r.finished
		r.t.Fatalf("local-data did not exit within %s:\n%s", timeout, r.output.String())
		return "", nil
	}
}

// waitForSignalReadyDaemon returns the PID of the shim invoked as name with
// args once it has installed its signal handler.
func (s *sandbox) waitForSignalReadyDaemon(name string, args []string, timeout time.Duration) int {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, record := range s.records() {
			if record.Name == name && equalArgs(record.Args, args) {
				for _, event := range s.signalEvents(record.PID) {
					if event == "ready" {
						return record.PID
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	s.t.Fatalf("%s %q did not become ready within %s; records: %#v", name, args, timeout, s.records())
	return 0
}

// signalEvents returns the shim's recorded signal events for pid, in order.
func (s *sandbox) signalEvents(pid int) []string {
	s.t.Helper()
	data, err := os.ReadFile(filepath.Join(s.baseDir, "shim-signals.log"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		s.t.Fatal(err)
	}
	var events []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var event struct {
			PID   int    `json:"pid"`
			Event string `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			s.t.Fatalf("decode shim signal event %q: %v", line, err)
		}
		if event.PID == pid {
			events = append(events, event.Event)
		}
	}
	return events
}

// invocationKey names a daemon by shim name and its first argument.
type invocationKey struct {
	name string
	arg  string
}

// matches reports whether record launched this daemon. A "--service" key
// matches any Hive daemon.
func (k invocationKey) matches(record invocation) bool {
	if record.Name != k.name {
		return false
	}
	if k.arg == "--service" {
		return argsHavePrefix(record.Args, []string{"--service"})
	}
	return len(record.Args) == 1 && record.Args[0] == k.arg
}

func (s *sandbox) daemonPID(key invocationKey) int {
	s.t.Helper()
	return s.waitForDaemonPID(key, 0, 5*time.Second)
}

// waitForDaemonPID returns the PID of the shim launched as key by a record at
// or after index from.
func (s *sandbox) waitForDaemonPID(key invocationKey, from int, timeout time.Duration) int {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		records := s.records()
		if from > len(records) {
			from = len(records)
		}
		for _, record := range records[from:] {
			if key.matches(record) {
				return record.PID
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	s.t.Fatalf("%s %s was not launched within %s", key.name, key.arg, timeout)
	return 0
}

// signalChildPID returns the descendant PID a hung shim recorded.
func (s *sandbox) signalChildPID(pid int) int {
	s.t.Helper()
	for _, event := range s.signalEvents(pid) {
		if value, found := strings.CutPrefix(event, "child="); found {
			child, err := strconv.Atoi(value)
			if err != nil {
				s.t.Fatalf("invalid child event %q: %v", event, err)
			}
			return child
		}
	}
	s.t.Fatalf("pid %d recorded no descendant", pid)
	return 0
}

// sigtermOrder returns pids in the order the shim log recorded their SIGTERM.
func (s *sandbox) sigtermOrder(pids []int) []int {
	s.t.Helper()
	wanted := make(map[int]bool, len(pids))
	for _, pid := range pids {
		wanted[pid] = true
	}
	data, err := os.ReadFile(filepath.Join(s.baseDir, "shim-signals.log"))
	if err != nil {
		s.t.Fatal(err)
	}
	var order []int
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var event struct {
			PID   int    `json:"pid"`
			Event string `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			s.t.Fatalf("decode shim signal event %q: %v", line, err)
		}
		if event.Event == "SIGTERM" && wanted[event.PID] {
			order = append(order, event.PID)
		}
	}
	return order
}

func hasEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

func equalInts(actual, expected []int) bool {
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

func assertNoPIDFiles(t *testing.T, baseDir string) {
	t.Helper()
	for _, serviceName := range []string{"hdfs", "yarn", "hive"} {
		matches, err := filepath.Glob(filepath.Join(baseDir, "state", serviceName, "pids", "*.pid"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 0 {
			t.Fatalf("stale PID files remain after rollback: %#v", matches)
		}
	}
}
