package hdfs

import (
	"time"

	"github.com/danieljhkim/local-data-platform/internal/service"
)

const hdfsStopTimeout = 5 * time.Second

var terminateHDFSPID = func(pid int) error {
	return terminateHDFSPIDWithOptions(pid, service.TerminateOptions{
		Timeout:   hdfsStopTimeout,
		IsRunning: IsProcessRunning,
	})
}

func terminateHDFSPIDWithOptions(pid int, options service.TerminateOptions) error {
	return service.TerminatePIDWithOptions(pid, options)
}
