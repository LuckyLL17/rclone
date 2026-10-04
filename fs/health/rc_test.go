package health

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rclone/rclone/fs/rc"
)

func callRC(t *testing.T, path string, in rc.Params) rc.Params {
	t.Helper()
	call := rc.Calls.Get(path)
	require.NotNil(t, call, "rc path %s registered", path)
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	return out
}

func TestRCStatusAndReset(t *testing.T) {
	_, err := defaultRegistry.Reset("")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = defaultRegistry.Reset("")
	})

	a := defaultRegistry.Circuit("a", Options{Failures: 1, Cooldown: 0})
	a.Record(currentPermit(a), false, io.EOF)
	defaultRegistry.Circuit("b", Options{Failures: 2})

	// Status of all remotes.
	out := callRC(t, "core/health/status", rc.Params{})
	statuses, ok := out["health"].([]Status)
	require.True(t, ok)
	require.Len(t, statuses, 2)
	assert.Equal(t, "a", statuses[0].Remote)
	assert.Equal(t, "open", statuses[0].State)
	assert.Equal(t, int64(1), statuses[0].OpenCount)
	assert.Equal(t, "b", statuses[1].Remote)
	assert.Equal(t, "closed", statuses[1].State)

	// Status of a single remote.
	out = callRC(t, "core/health/status", rc.Params{"fs": "b"})
	statuses = out["health"].([]Status)
	require.Len(t, statuses, 1)
	assert.Equal(t, "b", statuses[0].Remote)

	// Unknown remote is an error.
	call := rc.Calls.Get("core/health/status")
	_, err = call.Fn(context.Background(), rc.Params{"fs": "missing"})
	require.Error(t, err)

	// Reset a single remote: state cleared in place.
	out = callRC(t, "core/health/reset", rc.Params{"fs": "a"})
	assert.Equal(t, []string{"a"}, out["reset"])
	a2, exists := defaultRegistry.Get("a")
	assert.True(t, exists)
	assert.Equal(t, "closed", a2.Snapshot().State)
	assert.Equal(t, int64(0), a2.Snapshot().OpenCount)

	// Resetting an unknown remote is an error.
	call = rc.Calls.Get("core/health/reset")
	_, err = call.Fn(context.Background(), rc.Params{"fs": "missing"})
	require.Error(t, err)

	// Reset everything, leaving closed circuits with zeroed counters.
	out = callRC(t, "core/health/reset", rc.Params{})
	assert.Equal(t, []string{"a", "b"}, out["reset"])
	statuses = defaultRegistry.Snapshot()
	require.Len(t, statuses, 2)
	for _, s := range statuses {
		assert.Equal(t, "closed", s.State)
		assert.Equal(t, int64(0), s.OpenCount)
	}
}
