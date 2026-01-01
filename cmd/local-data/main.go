package main

import (
	"os"

	"github.com/danieljhkim/local-data-platform/internal/cli"
)

var version = "dev"

func main() {
	cli.SetVersion(version)

	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
