//go:build integration && live_smoke

package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLiveMacOSSmoke(t *testing.T) {
	if os.Getenv("LOCAL_DATA_LIVE_SMOKE") != "1" {
		t.Skip("set LOCAL_DATA_LIVE_SMOKE=1 to run against installed services")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("the live service smoke test is supported only on macOS")
	}
	for _, command := range []string{"brew", "hdfs", "hive", "schematool", "spark-submit"} {
		if _, err := exec.LookPath(command); err != nil {
			t.Skipf("live smoke prerequisite %q is unavailable: %v", command, err)
		}
	}

	home := t.TempDir()
	environment := liveEnvironment(home)
	run := func(input string, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, localDataBin, args...)
		cmd.Dir = repositoryRoot
		cmd.Env = environment
		cmd.Stdin = strings.NewReader(input)
		output, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("local-data %s timed out: %v\n%s", strings.Join(args, " "), ctx.Err(), output)
		}
		if err != nil {
			t.Fatalf("local-data %s failed: %v\n%s", strings.Join(args, " "), err, output)
		}
		return string(output)
	}
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, localDataBin, "stop")
		cmd.Dir = repositoryRoot
		cmd.Env = environment
		_ = cmd.Run()
	}
	defer stop()

	run("\n\n\n\n", "init", "--user", "integration-smoke", "--db-type", "derby")
	run("", "profile", "set", "local")
	run("", "start")
	run("", "hive", "-e", "SELECT 1")
	stop()

	run("", "profile", "set", "hdfs")
	run("", "start")

	localFile := filepath.Join(home, "hdfs-smoke.txt")
	want := "local-data live smoke\n"
	if err := os.WriteFile(localFile, []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	remoteFile := fmt.Sprintf("/tmp/local-data-smoke-%d.txt", time.Now().UnixNano())
	run("", "hdfs", "dfs", "-put", localFile, remoteFile)
	if got := run("", "hdfs", "dfs", "-cat", remoteFile); got != want {
		t.Fatalf("HDFS round trip = %q, want %q", got, want)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, localDataBin, "hdfs", "dfs", "-rm", "-f", remoteFile)
		cmd.Dir = repositoryRoot
		cmd.Env = environment
		_ = cmd.Run()
	})

	sparkJob := filepath.Join(home, "spark-smoke.py")
	job := "from pyspark.sql import SparkSession\n" +
		"spark = SparkSession.builder.getOrCreate()\n" +
		"assert spark.range(5).count() == 5\n" +
		"spark.stop()\n"
	if err := os.WriteFile(sparkJob, []byte(job), 0o600); err != nil {
		t.Fatal(err)
	}
	run("", "spark-submit", sparkJob)
}

func liveEnvironment(home string) []string {
	result := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key != "HOME" && key != "BASE_DIR" && key != "ACTIVE_PROFILE" {
			result = append(result, entry)
		}
	}
	return append(result, "HOME="+home, "USER=integration-smoke", "NO_COLOR=1")
}
