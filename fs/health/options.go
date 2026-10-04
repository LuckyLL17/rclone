// Package health implements an optional per-remote health circuit
// breaker.
//
// When enabled, a remote whose consecutive paced operations fail with
// retriable errors (timeouts, rate limits, server errors) is cut off
// for a cooldown period; new operations fail immediately instead of
// being retried against an unhealthy remote. After the cooldown a
// single probe operation is allowed through, and once it succeeds the
// remote is returned to service. Deterministic failures
// (authentication, permission, missing paths) never trip the breaker.
//
// The breaker hooks into backend API calls through the pacer gate
// registered on fs.NewPacerGate, so every command and rc call that
// uses a paced backend shares the same semantics without per-command
// changes.
package health

import (
	"time"

	"github.com/rclone/rclone/fs"
)

// Default values for the circuit breaker settings.
const (
	DefaultFailures     = 5
	DefaultCooldown     = 30 * time.Second
	DefaultProbeSuccess = 1
)

// Options configures a Circuit.
type Options struct {
	// Failures is the number of consecutive retriable failures which
	// opens the circuit.
	Failures int
	// Cooldown is how long the circuit stays open before a probe is
	// allowed.
	Cooldown time.Duration
	// ProbeSuccess is the number of consecutive successful probes in
	// the half-open state which closes the circuit.
	ProbeSuccess int
}

// DefaultOptions returns the built-in circuit breaker settings.
func DefaultOptions() Options {
	return Options{
		Failures:     DefaultFailures,
		Cooldown:     DefaultCooldown,
		ProbeSuccess: DefaultProbeSuccess,
	}
}

// OptionsFromConfig reads the circuit breaker settings from ci.
func OptionsFromConfig(ci *fs.ConfigInfo) Options {
	o := Options{
		Failures:     ci.HealthFailures,
		Cooldown:     time.Duration(ci.HealthCooldown),
		ProbeSuccess: ci.HealthProbeSuccess,
	}
	o.Normalize()
	return o
}

// Normalize replaces unset or invalid values with safe defaults.
func (o *Options) Normalize() {
	if o.Failures < 1 {
		o.Failures = DefaultFailures
	}
	if o.Cooldown < 0 {
		o.Cooldown = DefaultCooldown
	}
	if o.ProbeSuccess < 1 {
		o.ProbeSuccess = DefaultProbeSuccess
	}
}
