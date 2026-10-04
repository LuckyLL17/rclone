package fs

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rclone/rclone/lib/pacer"
)

func TestRemoteNameContext(t *testing.T) {
	ctx := context.Background()
	_, _, ok := RemoteName(ctx)
	assert.False(t, ok)

	ctx = WithRemoteName(ctx, "drive", "path/to/dir")
	name, root, ok := RemoteName(ctx)
	require.True(t, ok)
	assert.Equal(t, "drive", name)
	assert.Equal(t, "path/to/dir", root)
}

type stubGate struct{ err error }

func (g *stubGate) Before() (pacer.Permit, error)   { return nil, g.err }
func (g *stubGate) After(pacer.Permit, bool, error) {}

func TestNewPacerGateHook(t *testing.T) {
	oldHook := NewPacerGate
	defer func() { NewPacerGate = oldHook }()

	// A rejecting gate stops the paced function from running.
	gateErr := errors.New("circuit open")
	NewPacerGate = func(context.Context) pacer.Gate { return &stubGate{err: gateErr} }
	p := NewPacer(context.Background(), nil)
	calls := 0
	err := p.Call(func() (bool, error) {
		calls++
		return false, nil
	})
	require.ErrorIs(t, err, gateErr)
	assert.Equal(t, 0, calls)

	// A nil gate result leaves the pacer ungated.
	NewPacerGate = func(context.Context) pacer.Gate { return nil }
	p = NewPacer(context.Background(), nil)
	calls = 0
	require.NoError(t, p.Call(func() (bool, error) {
		calls++
		return false, nil
	}))
	assert.Equal(t, 1, calls)

	// No hook registered behaves as before.
	NewPacerGate = nil
	p = NewPacer(context.Background(), nil)
	calls = 0
	require.NoError(t, p.Call(func() (bool, error) {
		calls++
		return false, nil
	}))
	assert.Equal(t, 1, calls)
}
