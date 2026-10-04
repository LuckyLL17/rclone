// Pacer with logging and calculator

package fs

import (
	"context"
	"time"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/pacer"
)

// Pacer is a simple wrapper around a pacer.Pacer with logging.
type Pacer struct {
	*pacer.Pacer
}

type logCalculator struct {
	pacer.Calculator
}

// NewPacerGate, if non-nil, attaches a pacer.Gate returned for ctx to
// every Pacer created by NewPacer. It is the extension point used by
// the remote health circuit breaker; a nil result leaves the Pacer
// ungated. The default nil hook changes no behaviour.
var NewPacerGate func(ctx context.Context) pacer.Gate

// NewPacer creates a Pacer for the given Fs and Calculator.
func NewPacer(ctx context.Context, c pacer.Calculator) *Pacer {
	ci := GetConfig(ctx)
	retries := max(ci.LowLevelRetries, 1)
	maxConnections := max(ci.MaxConnections, 0)
	options := []pacer.Option{
		pacer.InvokerOption(pacerInvoker),
		pacer.MaxConnectionsOption(maxConnections),
		pacer.RetriesOption(retries),
		pacer.CalculatorOption(c),
	}
	if NewPacerGate != nil {
		if gate := NewPacerGate(ctx); gate != nil {
			options = append(options, pacer.GateOption(gate))
		}
	}
	p := &Pacer{
		Pacer: pacer.New(options...),
	}
	p.SetCalculator(c)
	return p
}

func (d *logCalculator) Calculate(state pacer.State) time.Duration {
	oldSleepTime := state.SleepTime
	newSleepTime := d.Calculator.Calculate(state)
	if state.ConsecutiveRetries > 0 {
		if newSleepTime != oldSleepTime {
			Debugf("pacer", "Rate limited, increasing sleep to %v", newSleepTime)
		}
	} else {
		if newSleepTime != oldSleepTime {
			Debugf("pacer", "Reducing sleep to %v", newSleepTime)
		}
	}
	return newSleepTime
}

// SetCalculator sets the pacing algorithm. Don't modify the Calculator object
// afterwards, use the ModifyCalculator method when needed.
//
// It will choose the default algorithm if nil is passed in.
func (p *Pacer) SetCalculator(c pacer.Calculator) {
	switch c.(type) {
	case *logCalculator:
		Logf("pacer", "Invalid Calculator in fs.Pacer.SetCalculator")
	case nil:
		c = &logCalculator{pacer.NewDefault()}
	default:
		c = &logCalculator{c}
	}

	p.Pacer.SetCalculator(c)
}

// ModifyCalculator calls the given function with the currently configured
// Calculator and the Pacer lock held.
func (p *Pacer) ModifyCalculator(f func(pacer.Calculator)) {
	p.Pacer.ModifyCalculator(func(c pacer.Calculator) {
		switch _c := c.(type) {
		case *logCalculator:
			f(_c.Calculator)
		default:
			Logf("pacer", "Invalid Calculator in fs.Pacer: %t", c)
			f(c)
		}
	})
}

func pacerInvoker(try, retries int, f pacer.Paced) (retry bool, err error) {
	retry, err = f()
	if retry {
		Debugf("pacer", "low level retry %d/%d (error %v)", try, retries, err)
		err = fserrors.RetryError(err)
	}
	return
}
