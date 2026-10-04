package health_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/health"
	"github.com/rclone/rclone/lib/pacer"
)

func wiredContext(t *testing.T) context.Context {
	t.Helper()
	ctx, ci := fs.AddConfig(context.Background())
	ci.HealthEnable = true
	ci.HealthFailures = 2
	ci.HealthCooldown = fs.Duration(60 * time.Millisecond)
	ci.HealthProbeSuccess = 1
	ci.LowLevelRetries = 1
	return fs.WithRemoteName(ctx, "wired-remote", "")
}

func newWiredPacer(ctx context.Context) *fs.Pacer {
	return fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(0), pacer.MaxSleep(time.Millisecond)))
}

func findStatus(t *testing.T, key string) health.Status {
	t.Helper()
	for _, s := range health.DefaultRegistry().Snapshot() {
		if s.Remote == key {
			return s
		}
	}
	t.Fatalf("circuit %q not found in registry", key)
	return health.Status{}
}

// A real fs.NewPacer drives the whole stack: pacer -> gate -> circuit
// -> registry.
func TestPacerWiringLifecycle(t *testing.T) {
	_, err := health.DefaultRegistry().Reset("")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = health.DefaultRegistry().Reset("")
	})

	ctx := wiredContext(t)
	p := newWiredPacer(ctx)

	// Two retriable calls trip the breaker (threshold 2).
	for range 2 {
		_ = p.Call(func() (bool, error) { return true, io.EOF })
	}
	assert.Equal(t, "open", findStatus(t, "wired-remote").State)

	// Open calls never reach the paced function.
	calls := 0
	err = p.Call(func() (bool, error) {
		calls++
		return false, nil
	})
	require.ErrorIs(t, err, health.ErrCircuitOpen)
	assert.Equal(t, 0, calls)

	// After the cooldown the next call is a successful probe which
	// closes the circuit.
	time.Sleep(100 * time.Millisecond)
	calls = 0
	require.NoError(t, p.Call(func() (bool, error) {
		calls++
		return false, nil
	}))
	assert.Equal(t, 1, calls)
	assert.Equal(t, "closed", findStatus(t, "wired-remote").State)
}

func TestPacerWiringSharedBetweenPacers(t *testing.T) {
	_, err := health.DefaultRegistry().Reset("")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = health.DefaultRegistry().Reset("")
	})

	ctx := wiredContext(t)
	p1 := newWiredPacer(ctx)
	p2 := newWiredPacer(ctx)

	for range 2 {
		_ = p1.Call(func() (bool, error) { return true, io.EOF })
	}

	calls := 0
	err = p2.Call(func() (bool, error) {
		calls++
		return false, nil
	})
	require.ErrorIs(t, err, health.ErrCircuitOpen)
	assert.Equal(t, 0, calls)
}

// A half-open probe which paces a nested call (as b2 does when it
// fetches an upload URL inside an upload attempt) must not be rejected
// by its own inner call; recovery still succeeds.
func TestPacerWiringNestedProbe(t *testing.T) {
	_, err := health.DefaultRegistry().Reset("")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = health.DefaultRegistry().Reset("")
	})

	ctx := wiredContext(t)
	outer := newWiredPacer(ctx)
	inner := newWiredPacer(ctx)

	for range 2 {
		_ = outer.Call(func() (bool, error) { return true, io.EOF })
	}
	require.Equal(t, "open", findStatus(t, "wired-remote").State)

	time.Sleep(100 * time.Millisecond)
	innerCalls := 0
	err = outer.Call(func() (bool, error) {
		if iErr := inner.Call(func() (bool, error) {
			innerCalls++
			return false, nil
		}); iErr != nil {
			return true, iErr
		}
		return false, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, innerCalls)
	assert.Equal(t, "closed", findStatus(t, "wired-remote").State)
}

func TestNewGateDisabledByDefault(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	assert.False(t, ci.HealthEnable)
	assert.Nil(t, health.NewGate(ctx))

	ctx = fs.WithRemoteName(ctx, "gated", "")
	assert.Nil(t, health.NewGate(ctx))

	ci.HealthEnable = true
	assert.NotNil(t, health.NewGate(ctx))

	// A context with no remote identity never gets a gate.
	other, oci := fs.AddConfig(context.Background())
	oci.HealthEnable = true
	assert.Nil(t, health.NewGate(other))
}
