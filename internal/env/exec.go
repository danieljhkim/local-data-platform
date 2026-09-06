package env

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/danieljhkim/local-data-platform/internal/config"
)

// Exec executes a command with the computed environment
// Mirrors ld_env_exec
func Exec(paths *config.Paths, args []string) error {
	return ExecWithEnv(paths, args, nil)
}

// ExecWithEnv executes a command with the computed environment plus extra env vars
func ExecWithEnv(paths *config.Paths, args []string, extraEnv map[string]string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: local-data env exec -- <cmd...>")
	}

	// Compute environment
	env, err := Compute(paths)
	if err != nil {
		return err
	}

	// Set environment (merged with current)
	cmdEnv := env.MergeWithCurrent()

	// Add extra environment variables
	for key, value := range extraEnv {
		cmdEnv = append(cmdEnv, key+"="+value)
	}
	cmd, err := Command(args[0], args[1:], cmdEnv)
	if err != nil {
		return err
	}

	// Connect stdio
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run and wait
	return cmd.Run()
}

// Command constructs a command that resolves a bare executable name using the
// PATH from commandEnv. os/exec resolves names when Command is called, before
// Cmd.Env is applied, so calling exec.Command directly would use the parent's
// PATH instead of the selected runtime installation.
func Command(name string, args []string, commandEnv []string) (*exec.Cmd, error) {
	return CommandContext(context.Background(), name, args, commandEnv)
}

// CommandContext is Command with cancellation support.
func CommandContext(ctx context.Context, name string, args []string, commandEnv []string) (*exec.Cmd, error) {
	path, err := ResolveExecutable(name, commandEnv)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = commandEnv
	return cmd, nil
}

// ResolveExecutable resolves a bare executable against PATH in commandEnv.
// Explicit paths are returned unchanged so callers can intentionally bypass
// PATH selection. A nil environment follows os/exec's parent-environment
// behavior.
func ResolveExecutable(name string, commandEnv []string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("executable name is empty")
	}
	if strings.ContainsRune(name, os.PathSeparator) {
		return name, nil
	}

	path := lookupEnv(commandEnv, "PATH")
	if commandEnv == nil {
		path = os.Getenv("PATH")
	}
	var nonExecutable string
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			dir = "."
		}
		candidate := filepath.Join(dir, name)
		if dir == "." {
			candidate = "." + string(os.PathSeparator) + name
		}
		info, err := os.Stat(candidate)
		if err != nil {
			continue
		}
		if info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
			return candidate, nil
		}
		if nonExecutable == "" {
			nonExecutable = candidate
		}
	}
	if nonExecutable != "" {
		return "", fmt.Errorf("executable %q on PATH is not executable: %s", name, nonExecutable)
	}
	return "", &exec.Error{Name: name, Err: exec.ErrNotFound}
}

func lookupEnv(commandEnv []string, key string) string {
	prefix := key + "="
	for i := len(commandEnv) - 1; i >= 0; i-- {
		if strings.HasPrefix(commandEnv[i], prefix) {
			return strings.TrimPrefix(commandEnv[i], prefix)
		}
	}
	return ""
}
