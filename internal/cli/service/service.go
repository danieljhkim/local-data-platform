package service

import (
	"github.com/danieljhkim/local-data-platform/internal/config"
	"github.com/spf13/cobra"
)

// PathsGetter is a function that returns the Paths instance
type PathsGetter func() *config.Paths

// NewStartCmd creates the start command
func NewStartCmd(pathsGetter PathsGetter) *cobra.Command {
	return newStartCmd(pathsGetter)
}

// NewStopCmd creates the stop command
func NewStopCmd(pathsGetter PathsGetter) *cobra.Command {
	return newStopCmd(pathsGetter)
}

// NewStatusCmd creates the status command
func NewStatusCmd(pathsGetter PathsGetter) *cobra.Command {
	return newStatusCmd(pathsGetter)
}
