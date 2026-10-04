package health

import (
	"context"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
)

func init() {
	// Invert the fs -> fs/health dependency: fs exposes the hook while
	// this package provides the implementation.
	fs.NewPacerGate = NewGate
}

// NewGate returns a pacer gate for the remote carried in ctx, or nil
// when the health circuit breaker is disabled or the context carries
// no remote identity.
func NewGate(ctx context.Context) pacer.Gate {
	ci := fs.GetConfig(ctx)
	if !ci.HealthEnable {
		return nil
	}
	name, root, ok := fs.RemoteName(ctx)
	if !ok {
		return nil
	}
	key := Key(name, root)
	if key == "" {
		return nil
	}
	circuit := defaultRegistry.Circuit(key, OptionsFromConfig(ci))
	return &pacedGate{circuit: circuit}
}

// pacedGate adapts the shared Circuit to the pacer.
type pacedGate struct {
	circuit *Circuit
}

// Before implements pacer.Gate.
func (g *pacedGate) Before() (pacer.Permit, error) {
	return g.circuit.Allow()
}

// After implements pacer.Gate.
func (g *pacedGate) After(permit pacer.Permit, retry bool, err error) {
	g.circuit.Record(permit, retry, err)
}
