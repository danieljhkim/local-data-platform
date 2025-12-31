package env

import (
	"fmt"
	"os"
	"os/exec"

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

	// Build command
	cmd := exec.Command(args[0], args[1:]...)

	// Set environment (merged with current)
	cmdEnv := env.MergeWithCurrent()

	// Add extra environment variables
	for key, value := range extraEnv {
		cmdEnv = append(cmdEnv, key+"="+value)
	}
	cmd.Env = cmdEnv

	// Connect stdio
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Run and wait
	return cmd.Run()
}
