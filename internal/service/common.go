package service

// ServiceStatus represents the status of a service or daemon
type ServiceStatus struct {
	Name    string // Service name (e.g., "namenode", "datanode", "resourcemanager")
	Running bool   // true if running
	PID     int    // Process ID (0 if not running)
}

// Service is the interface that all services must implement
type Service interface {
	Start() error
	Stop() error
	Status() ([]ServiceStatus, error)
	Logs() error
}
