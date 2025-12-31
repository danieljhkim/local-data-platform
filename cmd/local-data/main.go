package main

import (
	"os"

	"github.com/danieljhkim/local-data-platform/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
