package health

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
)

// State of a Circuit.
type State int

const (
	// Closed lets all calls through and counts consecutive failures.
	Closed State = iota
	// Open rejects every call until the cooldown elapses.
	Open
	// HalfOpen lets a single probe call through at a time.
	HalfOpen
)

// String returns the lower-case state name used in logs and rc output.
func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// ErrCircuitOpen is returned in place of contacting a remote whose
// health circuit breaker is open.
//
// It is marked NoRetry and NoLowLevelRetry so rejected calls fail fast
// instead of burning high or low level retries.
var ErrCircuitOpen = errors.New("remote health circuit breaker is open")

type circuitOpenError struct {
	remote string
	until  time.Time
}

func (e *circuitOpenError) Error() string {
	return fmt.Sprintf("%s: remote %q is cut off until %s (%s from now)",
		ErrCircuitOpen.Error(), e.remote, e.until.Format(time.RFC3339), time.Until(e.until).Round(time.Second))
}

func (e *circuitOpenError) Unwrap() error         { return ErrCircuitOpen }
func (e *circuitOpenError) NoRetry() bool         { return true }
func (e *circuitOpenError) NoLowLevelRetry() bool { return true }

// permit identifies a call Allow let through. Records only act on a
// permit of the circuit generation that issued it, so results of
// in-flight calls from before a transition cannot move the current
// probe state.
type permit struct {
	gen   uint64
	probe bool
}

// Status is a point-in-time snapshot of a Circuit for diagnostics.
type Status struct {
	Remote                string        `json:"remote"`
	State                 string        `json:"state"`
	Generation            uint64        `json:"generation"`
	FailuresThreshold     int           `json:"failuresThreshold"`
	ConsecutiveFailures   int           `json:"consecutiveFailures"`
	Cooldown              time.Duration `json:"cooldown"`
	OpenedAt              *time.Time    `json:"openedAt,omitempty"`
	CooldownUntil         *time.Time    `json:"cooldownUntil,omitempty"`
	ProbesInFlight        int           `json:"probesInFlight"`
	ProbeSuccesses        int           `json:"probeSuccesses"`
	ProbeSuccessNeeded    int           `json:"probeSuccessNeeded"`
	Successes             int64         `json:"successes"`
	RetriableFailures     int64         `json:"retriableFailures"`
	DeterministicFailures int64         `json:"deterministicFailures"`
	Rejected              int64         `json:"rejected"`
	OpenCount             int64         `json:"openCount"`
	LastError             string        `json:"lastError,omitempty"`
	LastFailureAt         *time.Time    `json:"lastFailureAt,omitempty"`
	LastStateChange       time.Time     `json:"lastStateChange"`
}

// Circuit is the per-remote health state machine.
//
// All methods are safe for concurrent use.
type Circuit struct {
	key string
	now func() time.Time

	mu                    sync.Mutex
	opt                   Options
	generation            uint64
	state                 State
	consecutiveFailures   int
	openUntil             time.Time
	probesInFlight        int
	probeSuccesses        int
	successes             int64
	retriableFailures     int64
	deterministicFailures int64
	rejected              int64
	openCount             int64
	openedAt              time.Time
	lastFailureAt         time.Time
	lastStateChange       time.Time
	lastError             string
}

func newCircuit(key string, opt Options, now func() time.Time) *Circuit {
	opt.Normalize()
	if now == nil {
		now = time.Now
	}
	return &Circuit{
		key:             key,
		now:             now,
		opt:             opt,
		lastStateChange: now(),
	}
}

// Key returns the registry key of the remote.
func (c *Circuit) Key() string { return c.key }

// State returns the current state.
func (c *Circuit) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// SetOptions updates the breaker parameters without changing its state
// or counters.
func (c *Circuit) SetOptions(opt Options) {
	opt.Normalize()
	c.mu.Lock()
	c.opt = opt
	c.mu.Unlock()
}

// Allow returns a permit when a call to the remote may proceed, or an
// error wrapping ErrCircuitOpen when the call must not be made.
//
// In the half-open state only one probe call at a time is allowed.
func (c *Circuit) Allow() (pacer.Permit, error) {
	c.mu.Lock()
	now := c.now()
	var notice string
	switch c.state {
	case Closed:
	case Open:
		if now.Before(c.openUntil) {
			c.rejected++
			err := c.openErrorLocked()
			c.mu.Unlock()
			fs.Debugf(c.key, "%v", err)
			return nil, err
		}
		notice = c.enterHalfOpenLocked(now)
		c.probesInFlight = 1
	case HalfOpen:
		if c.probesInFlight > 0 {
			c.rejected++
			err := c.openErrorLocked()
			c.mu.Unlock()
			fs.Debugf(c.key, "%v", err)
			return nil, err
		}
		c.probesInFlight = 1
	}
	issued := permit{gen: c.generation, probe: c.state == HalfOpen}
	c.mu.Unlock()
	if notice != "" {
		fs.Logf(c.key, "%s", notice)
	}
	return issued, nil
}

// Record reports the outcome of a call carrying the permit Allow
// returned.
//
// retry is the backend's own retry decision from the paced invoker; a
// call explicitly asking to be retried is treated as retriable.
func (c *Circuit) Record(p pacer.Permit, retry bool, err error) {
	perm, ok := p.(permit)
	kind := kindSuccess
	if err != nil {
		kind = Classify(err)
		if retry && kind != kindIgnore {
			kind = kindRetriable
		}
	}

	c.mu.Lock()
	now := c.now()
	c.recordTotalLocked(kind, err, now)

	notice := ""
	// A permit from a previous generation belongs to a call allowed
	// before a transition; its outcome updates totals but must never
	// move the current probe state.
	if ok && perm.gen == c.generation {
		switch {
		case kind == kindSuccess:
			notice = c.recordSuccessLocked(perm, now)
		case kind == kindIgnore:
			if perm.probe && c.probesInFlight > 0 {
				c.probesInFlight--
			}
		case kind == kindDeterministic:
			notice = c.recordDeterministicLocked(perm, now)
		case kind == kindRetriable:
			notice = c.recordRetriableLocked(perm, now)
		}
	}
	c.mu.Unlock()

	if notice != "" {
		fs.Logf(c.key, "%s", notice)
	}
}

// recordTotalLocked updates the cumulative counters for an outcome.
func (c *Circuit) recordTotalLocked(kind Kind, err error, now time.Time) {
	switch kind {
	case kindSuccess:
		c.successes++
	case kindRetriable, kindDeterministic:
		if kind == kindRetriable {
			c.retriableFailures++
		} else {
			c.deterministicFailures++
		}
		c.lastError = err.Error()
		c.lastFailureAt = now
	}
}

// recordSuccessLocked handles a success carrying a current permit.
func (c *Circuit) recordSuccessLocked(perm permit, now time.Time) (notice string) {
	c.consecutiveFailures = 0
	if !perm.probe {
		return ""
	}
	c.probesInFlight = 0
	if c.state != HalfOpen {
		return ""
	}
	c.probeSuccesses++
	if c.probeSuccesses >= c.opt.ProbeSuccess {
		return c.closeLocked(now)
	}
	return ""
}

func (c *Circuit) recordRetriableLocked(perm permit, now time.Time) (notice string) {
	if perm.probe {
		c.probesInFlight = 0
		if c.state == HalfOpen {
			return c.openLocked(now, "probe operation failed")
		}
		return ""
	}
	if c.state == Open {
		return ""
	}
	c.consecutiveFailures++
	if c.consecutiveFailures >= c.opt.Failures {
		return c.openLocked(now, "")
	}
	return ""
}

func (c *Circuit) recordDeterministicLocked(perm permit, now time.Time) (notice string) {
	// The remote answered: the request (auth, path, permission) is the
	// problem, so the failure streak is broken without opening the
	// breaker. A half-open probe stays half-open for further probing.
	c.consecutiveFailures = 0
	if perm.probe && c.probesInFlight > 0 {
		c.probesInFlight--
	}
	return ""
}

// openLocked enters Open, scheduling the end of the cooldown.
func (c *Circuit) openLocked(now time.Time, reason string) string {
	c.generation++
	c.state = Open
	c.openUntil = now.Add(c.opt.Cooldown)
	c.openedAt = now
	c.probesInFlight = 0
	c.probeSuccesses = 0
	c.openCount++
	c.lastStateChange = now
	if reason == "" {
		return fmt.Sprintf("Health circuit breaker opened after %d consecutive retriable failures (threshold %d); cutting off the remote until %s (cooldown %s)",
			c.consecutiveFailures, c.opt.Failures, c.openUntil.Format(time.RFC3339), c.opt.Cooldown)
	}
	return fmt.Sprintf("Health circuit breaker re-opened: %s; cutting off the remote until %s (cooldown %s)",
		reason, c.openUntil.Format(time.RFC3339), c.opt.Cooldown)
}

func (c *Circuit) enterHalfOpenLocked(now time.Time) string {
	c.generation++
	c.state = HalfOpen
	c.probesInFlight = 0
	c.probeSuccesses = 0
	c.openUntil = time.Time{}
	c.lastStateChange = now
	return "Health circuit breaker cooldown elapsed; allowing a probe operation through"
}

func (c *Circuit) closeLocked(now time.Time) string {
	c.generation++
	c.state = Closed
	c.consecutiveFailures = 0
	c.probesInFlight = 0
	c.probeSuccesses = 0
	c.openUntil = time.Time{}
	c.openedAt = time.Time{}
	c.lastStateChange = now
	return fmt.Sprintf("Health circuit breaker closed: remote recovered after %d successful probe(s)", c.opt.ProbeSuccess)
}

func (c *Circuit) openErrorLocked() error {
	return &circuitOpenError{remote: c.key, until: c.openUntil}
}

// resetLocked returns the circuit to its closed state and clears all
// counters, affecting pacers already holding the circuit.
func (c *Circuit) resetLocked(now time.Time) string {
	c.generation++
	c.state = Closed
	c.consecutiveFailures = 0
	c.openUntil = time.Time{}
	c.probesInFlight = 0
	c.probeSuccesses = 0
	c.successes = 0
	c.retriableFailures = 0
	c.deterministicFailures = 0
	c.rejected = 0
	c.openCount = 0
	c.openedAt = time.Time{}
	c.lastFailureAt = time.Time{}
	c.lastStateChange = now
	c.lastError = ""
	return "Health circuit breaker reset"
}

// Reset returns the circuit to its closed state and clears all
// counters.
func (c *Circuit) Reset() {
	c.mu.Lock()
	notice := c.resetLocked(c.now())
	c.mu.Unlock()
	fs.Logf(c.key, "%s", notice)
}

// Snapshot returns a point-in-time diagnostic copy of the circuit state.
func (c *Circuit) Snapshot() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Status{
		Remote:                c.key,
		State:                 c.state.String(),
		Generation:            c.generation,
		FailuresThreshold:     c.opt.Failures,
		ConsecutiveFailures:   c.consecutiveFailures,
		Cooldown:              c.opt.Cooldown,
		ProbesInFlight:        c.probesInFlight,
		ProbeSuccesses:        c.probeSuccesses,
		ProbeSuccessNeeded:    c.opt.ProbeSuccess,
		Successes:             c.successes,
		RetriableFailures:     c.retriableFailures,
		DeterministicFailures: c.deterministicFailures,
		Rejected:              c.rejected,
		OpenCount:             c.openCount,
		LastError:             c.lastError,
		LastStateChange:       c.lastStateChange,
	}
	if !c.openedAt.IsZero() {
		t := c.openedAt
		s.OpenedAt = &t
	}
	if !c.openUntil.IsZero() {
		t := c.openUntil
		s.CooldownUntil = &t
	}
	if !c.lastFailureAt.IsZero() {
		t := c.lastFailureAt
		s.LastFailureAt = &t
	}
	return s
}
