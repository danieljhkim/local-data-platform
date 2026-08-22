package util

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadSecretFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pw")
	if err := os.WriteFile(path, []byte("s3cret-value\n"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadSecretFile(path)
	if err != nil {
		t.Fatalf("ReadSecretFile: %v", err)
	}
	if got != "s3cret-value" {
		t.Fatalf("got %q", got)
	}
}

func TestReadSecret_NonTTYDoesNotEcho(t *testing.T) {
	out := &strings.Builder{}
	got, err := ReadSecret(strings.NewReader("s3cret-value\n"), out, "")
	if err != nil {
		t.Fatalf("ReadSecret: %v", err)
	}
	if got != "s3cret-value" {
		t.Fatalf("got %q", got)
	}
	if strings.Contains(out.String(), "s3cret-value") {
		t.Fatalf("secret leaked to prompt output: %q", out.String())
	}
}

func TestReadSecret_TTYUsesEchoOffReader(t *testing.T) {
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	origIs := isTerminalFn
	origRead := readTTYPasswordFn
	t.Cleanup(func() {
		isTerminalFn = origIs
		readTTYPasswordFn = origRead
	})

	called := false
	isTerminalFn = func(fd int) bool { return fd == int(f.Fd()) }
	readTTYPasswordFn = func(fd int) ([]byte, error) {
		called = true
		return []byte("s3cret-value"), nil
	}

	out := &strings.Builder{}
	got, err := ReadSecret(f, out, "Enter db-password: ")
	if err != nil {
		t.Fatalf("ReadSecret: %v", err)
	}
	if !called {
		t.Fatal("expected term.ReadPassword (echo disabled) to be used")
	}
	if got != "s3cret-value" {
		t.Fatalf("got %q", got)
	}
	if strings.Contains(out.String(), "s3cret-value") {
		t.Fatalf("secret leaked to prompt output: %q", out.String())
	}
	if !strings.Contains(out.String(), "Enter db-password: ") {
		t.Fatalf("missing prompt: %q", out.String())
	}
}

func TestReadSecretLine_PrefersBufferedReader(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("s3cret-value\n"))
	got, err := ReadSecretLine(reader, strings.NewReader("ignored\n"), nil)
	if err != nil {
		t.Fatalf("ReadSecretLine: %v", err)
	}
	if got != "s3cret-value" {
		t.Fatalf("got %q", got)
	}
}
