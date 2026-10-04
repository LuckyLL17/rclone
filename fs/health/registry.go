package health

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// maxKeyRootLength bounds the on-the-fly remote root used in circuit
// breaker keys so hosts/paths cannot grow registry keys unbounded.
const maxKeyRootLength = 256

// Key derives the circuit breaker key for a backend under
// construction.
//
// A configured remote is keyed by its config name, which is shared by
// every pacer the backend (and its wrappers) creates. An on-the-fly
// remote (":type:...") additionally carries the root so different
// hosts do not share a breaker. An empty name yields an empty key,
// meaning no breaker is attached.
func Key(name, root string) string {
	if name == "" {
		return ""
	}
	if strings.HasPrefix(name, ":") && root != "" {
		if len(root) > maxKeyRootLength {
			root = root[:maxKeyRootLength]
		}
		return name + ":" + root
	}
	return name
}

// Registry holds the Circuit for each remote key.
type Registry struct {
	mu       sync.RWMutex
	circuits map[string]*Circuit
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{circuits: make(map[string]*Circuit)}
}

// defaultRegistry is the process-wide registry used by pacers and the
// rc endpoints.
var defaultRegistry = NewRegistry()

// DefaultRegistry returns the process-wide circuit breaker registry.
func DefaultRegistry() *Registry { return defaultRegistry }

// Circuit returns the Circuit for key, creating it with opt on first
// use. Repeated calls for the same key share one Circuit; opt is
// applied to it so newly constructed backends refresh the settings.
func (r *Registry) Circuit(key string, opt Options) *Circuit {
	if key == "" {
		return nil
	}
	r.mu.RLock()
	c, ok := r.circuits[key]
	r.mu.RUnlock()
	if ok {
		c.SetOptions(opt)
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok = r.circuits[key]; ok {
		c.SetOptions(opt)
		return c
	}
	c = newCircuit(key, opt, time.Now)
	r.circuits[key] = c
	return c
}

// Get returns the Circuit for key if one exists.
func (r *Registry) Get(key string) (*Circuit, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.circuits[key]
	return c, ok
}

// Snapshot returns a snapshot of every circuit, ordered by remote key.
func (r *Registry) Snapshot() []Status {
	r.mu.RLock()
	keys := make([]string, 0, len(r.circuits))
	for key := range r.circuits {
		keys = append(keys, key)
	}
	r.mu.RUnlock()
	sort.Strings(keys)

	statuses := make([]Status, 0, len(keys))
	r.mu.RLock()
	for _, key := range keys {
		statuses = append(statuses, r.circuits[key].Snapshot())
	}
	r.mu.RUnlock()
	return statuses
}

// Reset returns the circuit for key to its closed state with all
// counters cleared, returning its name. With an empty key every
// circuit is reset and all names are returned. An unknown non-empty
// key returns an error.
//
// Existing pacers keep holding the same circuits, so a reset takes
// effect immediately without recreating backends.
func (r *Registry) Reset(key string) ([]string, error) {
	r.mu.RLock()
	if key != "" {
		c, ok := r.circuits[key]
		r.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("no health circuit breaker for remote %q", key)
		}
		c.Reset()
		return []string{key}, nil
	}
	names := make([]string, 0, len(r.circuits))
	for name, c := range r.circuits {
		names = append(names, name)
		c.Reset()
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names, nil
}
