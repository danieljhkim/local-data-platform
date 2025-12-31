package profiles

import (
	"fmt"
	"sort"

	"github.com/danieljhkim/local-data-platform/internal/config/schema"
)

// Profile defines a complete configuration profile
type Profile struct {
	Name        string
	Description string
	ConfigSet   *schema.ConfigSet
}

// Registry manages built-in profiles
type Registry struct {
	profiles map[string]*Profile
}

// NewRegistry creates a registry with all built-in profiles
func NewRegistry() *Registry {
	r := &Registry{
		profiles: make(map[string]*Profile),
	}

	// Register built-in profiles
	r.Register(HDFSProfile())
	r.Register(LocalProfile())

	return r
}

// Register adds a profile to the registry
func (r *Registry) Register(p *Profile) {
	r.profiles[p.Name] = p
}

// Get retrieves a profile by name
func (r *Registry) Get(name string) (*Profile, error) {
	p, ok := r.profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown profile: %s", name)
	}
	return p, nil
}

// Has checks if a profile exists in the registry
func (r *Registry) Has(name string) bool {
	_, ok := r.profiles[name]
	return ok
}

// List returns all available profile names (sorted)
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.profiles))
	for name := range r.profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
