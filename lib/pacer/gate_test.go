package pacer

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordGate struct {
	mu      sync.Mutex
	befores int
	results []gateResult
	err     error
}

type gateResult struct {
	permit Permit
	retry  bool
	err    error
}

type testPermit struct{ id int }

func (g *recordGate) Before() (Permit, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.befores++
	if g.err != nil {
		return nil, g.err
	}
	return testPermit{id: g.befores}, nil
}

func (g *recordGate) After(permit Permit, retry bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.results = append(g.results, gateResult{permit: permit, retry: retry, err: err})
}

func (g *recordGate) snapshot() (int, []gateResult) {
	g.mu.Lock()
	defer g.mu.Unlock()
	results := make([]gateResult, len(g.results))
	copy(results, g.results)
	return g.befores, results
}

// A rejecting gate must stop the call before the paced function runs
// and must not use up any retries.
func TestGateReject(t *testing.T) {
	gateErr := errors.New("circuit open")
	gate := &recordGate{err: gateErr}
	p := New(
		RetriesOption(5),
		GateOption(gate),
	)
	calls := 0
	err := p.Call(func() (bool, error) {
		calls++
		return true, errors.New("should not run")
	})
	require.ErrorIs(t, err, gateErr)
	assert.Equal(t, 0, calls)
	befores, results := gate.snapshot()
	assert.Equal(t, 1, befores)
	assert.Empty(t, results)
}

// After receives one report per attempt, carrying the Before permit.
func TestGateReports(t *testing.T) {
	gate := &recordGate{}
	p := New(
		RetriesOption(3),
		CalculatorOption(&ZeroDelayCalculator{}),
		GateOption(gate),
	)
	err1 := errors.New("rate limited")
	calls := 0
	err := p.Call(func() (bool, error) {
		calls++
		if calls == 1 {
			return true, err1
		}
		return false, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	_, results := gate.snapshot()
	require.Len(t, results, 2)
	assert.Equal(t, gateResult{permit: testPermit{id: 1}, retry: true, err: err1}, results[0])
	assert.Equal(t, gateResult{permit: testPermit{id: 2}, retry: false, err: nil}, results[1])
}

// Once the gate starts rejecting halfway through the retries the call
// ends immediately with the gate error.
func TestGateRejectsMidCall(t *testing.T) {
	gate := &switchGate{}
	p := New(
		RetriesOption(10),
		CalculatorOption(&ZeroDelayCalculator{}),
		GateOption(gate),
	)
	gateErr := errors.New("opened")
	calls := 0
	err := p.Call(func() (bool, error) {
		calls++
		if calls == 2 {
			gate.err = gateErr
		}
		return true, errors.New("500")
	})
	require.ErrorIs(t, err, gateErr)
	assert.Equal(t, 2, calls)
}

// A paced call nested inside another one skips the gate entirely: the
// outer call's permit covers the whole operation.
func TestGateNestedCallSkipped(t *testing.T) {
	outerGate := &recordGate{}
	innerGate := &recordGate{}
	outer := New(RetriesOption(1), CalculatorOption(&ZeroDelayCalculator{}), GateOption(outerGate))
	inner := New(RetriesOption(1), CalculatorOption(&ZeroDelayCalculator{}), GateOption(innerGate))

	innerCalls := 0
	err := outer.Call(func() (bool, error) {
		if innerErr := inner.Call(func() (bool, error) {
			innerCalls++
			return false, nil
		}); innerErr != nil {
			return true, innerErr
		}
		return false, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, innerCalls)

	outerBefores, outerResults := outerGate.snapshot()
	innerBefores, innerResults := innerGate.snapshot()
	assert.Equal(t, 1, outerBefores)
	require.Len(t, outerResults, 1)
	assert.False(t, outerResults[0].retry)
	assert.Equal(t, 0, innerBefores)
	assert.Empty(t, innerResults)
}

type switchGate struct {
	err error
}

func (g *switchGate) Before() (Permit, error)   { return nil, g.err }
func (g *switchGate) After(Permit, bool, error) {}
