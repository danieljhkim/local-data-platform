package service

// ServiceStatus represents the status of a service or daemon
type ServiceStatus struct {
	Name    string // Service name (e.g., "namenode", "datanode", "resourcemanager")
	Running bool   // true if running
	PID     int    // Process ID (0 if not running)
	// ProbeError records a failed status observation. A false Running value
	// without ProbeError is an observed stopped process, not a failed probe.
	ProbeError error
}

// Service is the interface that all services must implement
type Service interface {
	Start() error
	Stop() error
	Status() ([]ServiceStatus, error)
	Logs() error
}
