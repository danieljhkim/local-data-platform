package main

import (
	"os"

	"github.com/danieljhkim/local-data-platform/internal/cli"
	envpkg "github.com/danieljhkim/local-data-platform/internal/env"
)

var version = "dev"

func main() {
	cli.SetVersion(version)

	if err := cli.Execute(); err != nil {
		os.Exit(envpkg.ExitCode(err))
	}
}
