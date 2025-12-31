package wrappers

import "github.com/danieljhkim/local-data-platform/internal/config"

// PathsGetter is a function that returns the Paths instance
type PathsGetter func() *config.Paths
