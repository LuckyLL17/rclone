package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	assert.Equal(t, "", Key("", ""))
	assert.Equal(t, "drive", Key("drive", "path"))
	assert.Equal(t, ":sftp:host:dir", Key(":sftp", "host:dir"))
	assert.Equal(t, ":sftp", Key(":sftp", ""))
}

func TestRegistryIsolationAndSharing(t *testing.T) {
	r := NewRegistry()
	opt := Options{Failures: 1, Cooldown: time.Minute}

	a := r.Circuit("a", opt)
	b := r.Circuit("b", opt)
	record(a, retriableErr())

	assert.Equal(t, Open, a.State())
	assert.Equal(t, Closed, b.State())

	// Same key shares state and refreshes options.
	aAgain := r.Circuit("a", Options{Failures: 4, Cooldown: time.Hour})
	assert.Same(t, a, aAgain)
	assert.Equal(t, Open, aAgain.State())
	assert.Equal(t, 4, aAgain.Snapshot().FailuresThreshold)

	statuses := r.Snapshot()
	require.Len(t, statuses, 2)
	assert.Equal(t, "a", statuses[0].Remote)
	assert.Equal(t, "b", statuses[1].Remote)
}

func TestRegistryReset(t *testing.T) {
	r := NewRegistry()
	a := r.Circuit("a", Options{Failures: 1})
	record(a, retriableErr())
	r.Circuit("b", Options{Failures: 1})
	require.Equal(t, Open, a.State())

	_, err := r.Reset("unknown")
	require.Error(t, err)

	// Reset clears state in place, so circuit holders see it at once.
	names, err := r.Reset("a")
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, names)
	aAgain, ok := r.Get("a")
	assert.True(t, ok)
	assert.Same(t, a, aAgain)
	assert.Equal(t, Closed, a.State())
	assert.Equal(t, int64(0), a.Snapshot().OpenCount)
	assert.Equal(t, int64(0), a.Snapshot().RetriableFailures)
	_, ok = r.Get("b")
	assert.True(t, ok)

	// Resetting an unknown remote is still an error.
	_, err = r.Reset("missing")
	require.Error(t, err)

	// Reset everything, keeping the entries in a closed state.
	names, err = r.Reset("")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, names)
	statuses := r.Snapshot()
	require.Len(t, statuses, 2)
	for _, s := range statuses {
		assert.Equal(t, "closed", s.State)
		assert.Equal(t, int64(0), s.OpenCount)
	}
}
