package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
)

type fakeClock struct{ t time.Time }

func newTestCircuit(t *testing.T, opt Options) (*Circuit, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	c := newCircuit("testremote", opt, func() time.Time { return clock.t })
	return c, clock
}

// currentPermit builds the permit Allow would issue in the current
// state, for tests which drive Record directly.
func currentPermit(c *Circuit) permit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return permit{gen: c.generation, probe: c.state == HalfOpen}
}

// priorPermit builds a permit from before the latest transition.
func priorPermit(c *Circuit) permit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return permit{gen: c.generation - 1, probe: false}
}

func record(c *Circuit, err error) {
	c.Record(currentPermit(c), false, err)
}

func retriableErr() error { return io.EOF }

func deterministicErr() error {
	return fserrors.NoRetryError(fs.ErrorPermissionDenied)
}

func TestOpensAfterConsecutiveRetriableFailures(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 3, Cooldown: 30 * time.Second, ProbeSuccess: 1})

	p, err := c.Allow()
	require.NoError(t, err)
	c.Record(p, false, retriableErr())
	c.Record(currentPermit(c), false, retriableErr())
	assert.Equal(t, Closed, c.State())

	c.Record(currentPermit(c), false, retriableErr())
	assert.Equal(t, Open, c.State())

	s := c.Snapshot()
	assert.Equal(t, "open", s.State)
	assert.Equal(t, int64(1), s.OpenCount)
	assert.Equal(t, 3, s.ConsecutiveFailures)
	assert.Equal(t, int64(3), s.RetriableFailures)
	assert.Equal(t, clock.t.Add(30*time.Second), *s.CooldownUntil)
	assert.NotEmpty(t, s.LastError)
}

func TestSuccessResetsFailureStreak(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 3})
	record(c, retriableErr())
	record(c, retriableErr())
	c.Record(currentPermit(c), false, nil)
	assert.Equal(t, 0, c.Snapshot().ConsecutiveFailures)
	record(c, retriableErr())
	record(c, retriableErr())
	assert.Equal(t, Closed, c.State())
}

func TestDeterministicFailuresNeverOpen(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 2})
	for range 100 {
		p, err := c.Allow()
		require.NoError(t, err)
		c.Record(p, false, deterministicErr())
	}
	assert.Equal(t, Closed, c.State())
	s := c.Snapshot()
	assert.Equal(t, int64(100), s.DeterministicFailures)
	assert.Equal(t, int64(0), s.RetriableFailures)
	assert.Equal(t, 0, s.ConsecutiveFailures)

	// A deterministic failure also breaks an in-progress streak.
	record(c, retriableErr())
	record(c, deterministicErr())
	assert.Equal(t, 0, c.Snapshot().ConsecutiveFailures)
}

func TestOpenRejectsCalls(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 1, Cooldown: time.Minute})
	record(c, retriableErr())
	require.Equal(t, Open, c.State())

	for range 3 {
		p, err := c.Allow()
		require.Error(t, err)
		require.Nil(t, p)
		require.ErrorIs(t, err, ErrCircuitOpen)
		assert.Contains(t, err.Error(), "testremote")
		assert.True(t, fserrors.IsNoRetryError(err))
		assert.True(t, fserrors.IsNoLowLevelRetryError(err))
	}
	assert.Equal(t, int64(3), c.Snapshot().Rejected)
}

func TestHalfOpenSingleProbeAndRecovery(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 1, Cooldown: 30 * time.Second, ProbeSuccess: 2})
	record(c, retriableErr())
	require.Equal(t, Open, c.State())
	oldUntil := *c.Snapshot().CooldownUntil

	// Still cooling down: rejected.
	_, err := c.Allow()
	require.ErrorIs(t, err, ErrCircuitOpen)

	// Cooldown elapsed: exactly one probe is allowed.
	clock.t = oldUntil.Add(time.Second)
	probe, err := c.Allow()
	require.NoError(t, err)
	assert.Equal(t, HalfOpen, c.State())
	_, err = c.Allow()
	require.ErrorIs(t, err, ErrCircuitOpen)
	_, err = c.Allow()
	require.ErrorIs(t, err, ErrCircuitOpen)

	// One success is not enough with ProbeSuccess=2.
	c.Record(probe, false, nil)
	assert.Equal(t, HalfOpen, c.State())
	probe2, err := c.Allow()
	require.NoError(t, err)
	c.Record(probe2, false, nil)
	assert.Equal(t, Closed, c.State())
	s := c.Snapshot()
	assert.Equal(t, int64(1), s.OpenCount)
	assert.Nil(t, s.CooldownUntil)
}

func TestHalfOpenProbeFailureReopens(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 1, Cooldown: 30 * time.Second})
	record(c, retriableErr())
	firstUntil := c.Snapshot().CooldownUntil
	clock.t = firstUntil.Add(time.Second)

	probe, err := c.Allow()
	require.NoError(t, err)
	assert.Equal(t, HalfOpen, c.State())
	c.Record(probe, false, retriableErr())
	assert.Equal(t, Open, c.State())

	s := c.Snapshot()
	require.NotNil(t, s.CooldownUntil)
	assert.True(t, s.CooldownUntil.After(*firstUntil), "cooldown must be reset into the future")
	assert.Equal(t, int64(2), s.OpenCount)
}

func TestHalfOpenDeterministicProbeKeepsHalfOpen(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 1, Cooldown: time.Minute})
	record(c, retriableErr())
	clock.t = c.Snapshot().CooldownUntil.Add(time.Second)

	probe, err := c.Allow()
	require.NoError(t, err)
	c.Record(probe, false, deterministicErr())
	assert.Equal(t, HalfOpen, c.State())
	assert.Equal(t, 0, c.Snapshot().ProbesInFlight)
	// Another probe can go through afterwards.
	_, err = c.Allow()
	require.NoError(t, err)
}

func TestHalfOpenIgnoredProbeReleasesSlot(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 1, Cooldown: time.Minute})
	record(c, retriableErr())
	clock.t = c.Snapshot().CooldownUntil.Add(time.Second)

	probe, err := c.Allow()
	require.NoError(t, err)
	c.Record(probe, false, context.Canceled)
	assert.Equal(t, HalfOpen, c.State())
	_, err = c.Allow()
	require.NoError(t, err)
}

// Results of calls allowed before a transition update totals but must
// not move the state of a later half-open probe generation.
func TestStalePermitsDoNotAffectProbe(t *testing.T) {
	c, clock := newTestCircuit(t, Options{Failures: 1, Cooldown: time.Minute, ProbeSuccess: 1})
	stale := currentPermit(c)
	record(c, retriableErr())
	require.Equal(t, Open, c.State())

	clock.t = c.Snapshot().CooldownUntil.Add(time.Second)
	probe, err := c.Allow()
	require.NoError(t, err)
	require.Equal(t, HalfOpen, c.State())
	gen := c.Snapshot().Generation

	// A stale success cannot close the circuit while the probe runs.
	c.Record(stale, false, nil)
	assert.Equal(t, HalfOpen, c.State())
	assert.Equal(t, 1, c.Snapshot().ProbesInFlight)
	assert.Equal(t, 0, c.Snapshot().ProbeSuccesses)
	_, err = c.Allow()
	require.ErrorIs(t, err, ErrCircuitOpen)

	// A stale retriable failure cannot extend/re-open the cooldown.
	c.Record(priorPermit(c), false, retriableErr())
	assert.Equal(t, HalfOpen, c.State())
	assert.Equal(t, gen, c.Snapshot().Generation)
	assert.Equal(t, int64(1), c.Snapshot().OpenCount)
	assert.Equal(t, 1, c.Snapshot().ProbesInFlight)

	// A stale cancel cannot release the probe slot.
	c.Record(priorPermit(c), false, context.Canceled)
	assert.Equal(t, 1, c.Snapshot().ProbesInFlight)
	_, err = c.Allow()
	require.ErrorIs(t, err, ErrCircuitOpen)

	// A stale deterministic result cannot release the slot either.
	c.Record(priorPermit(c), false, deterministicErr())
	assert.Equal(t, 1, c.Snapshot().ProbesInFlight)

	// The genuine probe recovers the circuit.
	c.Record(probe, false, nil)
	assert.Equal(t, Closed, c.State())
}

// A permit issued in closed state becomes stale once the circuit
// opens; its late result cannot reset the failure streak.
func TestStaleClosedPermitAfterOpen(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 1})
	stale := currentPermit(c)
	record(c, retriableErr())
	require.Equal(t, Open, c.State())

	c.Record(stale, false, nil)
	s := c.Snapshot()
	assert.Equal(t, "open", s.State)
	assert.Equal(t, 1, s.ConsecutiveFailures)
	assert.Equal(t, int64(1), s.Successes)
}

func TestRetryFlagOverridesClassification(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 1})
	c.Record(currentPermit(c), true, errors.New("backend asks to retry"))
	assert.Equal(t, Open, c.State())
}

func TestCircuitConcurrent(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 5, Cooldown: time.Hour})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := c.Allow(); err != nil {
					assert.ErrorIs(t, err, ErrCircuitOpen)
				}
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				record(c, retriableErr())
			}
		}()
	}
	wg.Wait()

	s := c.Snapshot()
	assert.Equal(t, "open", s.State)
	assert.Equal(t, int64(400), s.RetriableFailures)
	assert.Greater(t, s.Rejected, int64(0))
}

func TestOpenErrorWrapsSentinel(t *testing.T) {
	c, _ := newTestCircuit(t, Options{Failures: 1, Cooldown: time.Second})
	record(c, retriableErr())
	_, err := c.Allow()
	require.Error(t, err)
	var target *circuitOpenError
	require.True(t, errors.As(err, &target))
	assert.Equal(t, "testremote", target.remote)
	assert.NotZero(t, target.until)
	assert.Equal(t, fmt.Sprintf("%v", err), err.Error())
}
