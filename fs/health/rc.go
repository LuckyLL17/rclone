package health

import (
	"context"
	"fmt"

	"github.com/rclone/rclone/fs/rc"
)

func init() {
	rc.Add(rc.Call{
		Path:  "core/health/status",
		Fn:    rcStatus,
		Title: "Show the status of the remote health circuit breakers",
		Help: `This returns the state and counters of the per-remote health
circuit breakers (see the --health-enable and related flags).

This takes the following parameters:

- fs - optional remote name to show; without it every known remote is
  shown. Use the config name for configured remotes (e.g. "drive");
  on-the-fly remotes use the composite key shown in the "remote" field
  of this output.

Returns:

- health - array of status objects, one per remote, containing:
    - remote - config name (or composite on-the-fly key) of the remote
    - state - "closed", "open" or "half-open"
    - generation - state generation; results of older in-flight calls are ignored by the current state
    - failuresThreshold - consecutive retriable failures which open it
    - consecutiveFailures - current consecutive retriable failure count
    - cooldown - cooldown duration in nanoseconds
    - openedAt - when the circuit last opened (RFC3339, omitted if closed)
    - cooldownUntil - when a probe will be allowed (RFC3339, omitted if closed)
    - probesInFlight - probe calls currently allowed through
    - probeSuccesses - consecutive successful probes so far
    - probeSuccessNeeded - successes required to close the circuit
    - successes - total successful calls
    - retriableFailures - total retriable (timeout/rate limit/server) failures
    - deterministicFailures - total auth/path/permission failures
    - rejected - calls rejected while open
    - openCount - number of times the circuit has opened
    - lastError - text of the last recorded failure
    - lastFailureAt - time of the last recorded failure (RFC3339)
    - lastStateChange - time of the last state change (RFC3339)
`,
	})
	rc.Add(rc.Call{
		Path:  "core/health/reset",
		Fn:    rcReset,
		Title: "Reset remote health circuit breakers",
		Help: `This resets health circuit breakers to the closed state and clears
their counters.

This takes the following parameters:

- fs - optional remote name to reset; without it every circuit breaker
  is reset.

Returns:

- reset - array of remote names which were reset
`,
	})
}

func rcStatus(_ context.Context, in rc.Params) (rc.Params, error) {
	remote, err := in.GetString("fs")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	if remote == "" {
		return rc.Params{"health": defaultRegistry.Snapshot()}, nil
	}
	circuit, ok := defaultRegistry.Get(remote)
	if !ok {
		return nil, fmt.Errorf("no health circuit breaker for remote %q", remote)
	}
	return rc.Params{"health": []Status{circuit.Snapshot()}}, nil
}

func rcReset(_ context.Context, in rc.Params) (rc.Params, error) {
	remote, err := in.GetString("fs")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}
	names, err := defaultRegistry.Reset(remote)
	if err != nil {
		return nil, err
	}
	return rc.Params{"reset": names}, nil
}
