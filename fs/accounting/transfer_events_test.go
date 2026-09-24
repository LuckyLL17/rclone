package accounting

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newEventTestStats returns a fresh stats group with transfer events
// enabled and the given history size.
func newEventTestStats(t *testing.T, history int) (context.Context, *StatsInfo) {
	t.Helper()
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferEvents = true
	ci.TransferEventsHistory = history
	ci.TransferEventsProgress = fs.Duration(time.Second) // keep progress quiet by default
	group := "test-events-" + t.Name()
	ctx = WithStatsGroup(ctx, group)
	return ctx, Stats(ctx)
}

func eventTypes(events []TransferEvent) []TransferEventType {
	out := make([]TransferEventType, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}

func TestTransferEventsDisabledByDefault(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	assert.False(t, ci.TransferEvents)
	stats := NewStats(ctx)
	assert.Nil(t, stats.Events())

	// Emissions must be harmless no ops
	stats.QueuedTransferEvent(ctx, "file", 10, nil, nil, "", "")
	tr := stats.NewTransferRemoteSize("file", 10, nil, nil)
	tr.Done(ctx, nil)
	stats.EventsFinish(ctx, context.Canceled)
}

func TestTransferEventsCompletedLifecycle(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)

	stats.QueuedTransferEvent(ctx, "dir/new.txt", 42, nil, nil, "md5", "hashy")
	tr := stats.NewTransferRemoteSize("dir/new.txt", 42, nil, nil)
	tr.Done(ctx, nil)

	events := stats.Events().Query(EventFilter{}, 0, 0)
	require.Equal(t, []TransferEventType{
		TransferEventQueued,
		TransferEventStarted,
		TransferEventCompleted,
	}, eventTypes(events))

	assert.Equal(t, "dir/new.txt", events[0].Object)
	assert.Equal(t, int64(42), events[0].Size)
	// sequence numbers are monotonic
	assert.Less(t, events[0].Seq, events[1].Seq)
	assert.Less(t, events[1].Seq, events[2].Seq)
	// hash is carried from queued to started/completed
	assert.Equal(t, "md5", events[1].HashType)
	assert.Equal(t, "hashy", events[1].Hash)
	assert.Equal(t, "hashy", events[2].Hash)
}

func TestTransferEventsSkippedAndFailed(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)

	// up to date file checked and skipped
	checkTr := stats.NewCheckingTransfer(mockobject.New("equal.txt"), "checking")
	checkTr.SetHash("md5", "eq")
	checkTr.Skipped(ctx)
	checkTr.Done(ctx, nil)

	// failed check emits failed
	failCheck := stats.NewCheckingTransfer(mockobject.New("broken.txt"), "checking")
	checkErr := errors.New("check boom")
	failCheck.Done(ctx, checkErr)

	// failed transfer
	tr := stats.NewTransferRemoteSize("new.txt", 1, nil, nil)
	tr.Done(ctx, errors.New("copy boom"))

	events := stats.Events().Query(EventFilter{}, 0, 0)
	require.Equal(t, []TransferEventType{
		TransferEventSkipped,
		TransferEventFailed,
		TransferEventStarted,
		TransferEventFailed,
	}, eventTypes(events))

	assert.Equal(t, "equal.txt", events[0].Object)
	assert.Equal(t, "eq", events[0].Hash)
	assert.Equal(t, "check boom", events[1].Error)
	assert.Equal(t, "copy boom", events[3].Error)
}

func TestTransferEventsRetried(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)
	tr := stats.NewTransferRemoteSize("flaky.txt", 3, nil, nil)
	tr.Retried(ctx, errors.New("try again"), 2)
	tr.Done(ctx, nil)

	events := stats.Events().Query(EventFilter{}, 0, 0)
	require.Equal(t, []TransferEventType{
		TransferEventStarted,
		TransferEventRetried,
		TransferEventCompleted,
	}, eventTypes(events))
	assert.Equal(t, 2, events[1].Attempt)
	assert.Equal(t, "try again", events[1].Error)
}

func TestTransferEventsHistoryRingAndReplay(t *testing.T) {
	ctx, stats := newEventTestStats(t, 3)
	for i := range 5 {
		stats.QueuedTransferEvent(ctx, "f", int64(i), nil, nil, "", "")
	}
	events := stats.Events().Query(EventFilter{}, 0, 0)
	require.Len(t, events, 3)
	// only the last three are kept, oldest first
	assert.Equal(t, int64(2), events[0].Size)
	assert.Equal(t, int64(3), events[1].Size)
	assert.Equal(t, int64(4), events[2].Size)

	// replay with a cursor
	cursor := events[1].Seq
	replayed := stats.Events().Query(EventFilter{}, cursor, 0)
	require.Len(t, replayed, 1)
	assert.Equal(t, int64(4), replayed[0].Size)
}

func TestTransferEventsFiltersAndLimit(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)
	stats.QueuedTransferEvent(ctx, "dir/a.txt", 1, nil, nil, "", "")
	stats.QueuedTransferEvent(ctx, "dir/b.jpg", 2, nil, nil, "", "")
	stats.SkippedTransferEvent(ctx, "dir/a.txt", 1, nil, nil, "", "")

	onlyA, err := NewEventFilter("dir/a.txt", nil)
	require.NoError(t, err)
	assert.Len(t, stats.Events().Query(onlyA, 0, 0), 2)

	jpg, err := NewEventFilter("*.jpg", nil)
	require.NoError(t, err)
	got := stats.Events().Query(jpg, 0, 0)
	require.Len(t, got, 1)
	assert.Equal(t, "dir/b.jpg", got[0].Object)

	skippedOnly, err := NewEventFilter("", []string{"skipped"})
	require.NoError(t, err)
	got = stats.Events().Query(skippedOnly, 0, 0)
	require.Len(t, got, 1)
	assert.Equal(t, TransferEventSkipped, got[0].Type)

	assert.Len(t, stats.Events().Query(EventFilter{}, 0, 2), 2)

	_, err = NewEventFilter("", []string{"nonsense"})
	require.Error(t, err)
}

func TestTransferEventsSubscribe(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)
	events, done, cancel := stats.SubscribeTransferEvents()

	stats.QueuedTransferEvent(ctx, "live.txt", 1, nil, nil, "", "")
	select {
	case e := <-events:
		assert.Equal(t, TransferEventQueued, e.Type)
	case <-time.After(time.Second):
		t.Fatal("did not receive event")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription not cancelled")
	}

	// events after cancel do not block or panic
	stats.QueuedTransferEvent(ctx, "live2.txt", 1, nil, nil, "", "")
}

func TestTransferEventsFinish(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		ctx, stats := newEventTestStats(t, 100)
		stats.QueuedTransferEvent(ctx, "stuck1.txt", 1, nil, nil, "", "")
		stats.QueuedTransferEvent(ctx, "stuck2.txt", 2, nil, nil, "", "")
		tr := stats.NewTransferRemoteSize("stuck3.txt", 3, nil, nil)
		_ = tr

		stats.EventsFinish(ctx, context.Canceled)

		events := stats.Events().Query(EventFilter{}, 0, 0)
		var failed []string
		for _, e := range events {
			if e.Type == TransferEventFailed {
				assert.Equal(t, context.Canceled.Error(), e.Error)
				failed = append(failed, e.Object)
			}
		}
		assert.ElementsMatch(t, []string{"stuck1.txt", "stuck2.txt", "stuck3.txt"}, failed)

		// finish is idempotent
		stats.EventsFinish(ctx, context.Canceled)
	})

	t.Run("successful", func(t *testing.T) {
		ctx, stats := newEventTestStats(t, 100)
		stats.QueuedTransferEvent(ctx, "ok.txt", 1, nil, nil, "", "")
		tr := stats.NewTransferRemoteSize("ok.txt", 1, nil, nil)
		tr.Done(ctx, nil)

		stats.EventsFinish(ctx, nil)

		events := stats.Events().Query(EventFilter{}, 0, 0)
		assert.Equal(t, []TransferEventType{
			TransferEventQueued,
			TransferEventStarted,
			TransferEventCompleted,
		}, eventTypes(events))
	})
}

func TestTransferEventsProgress(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferEvents = true
	ci.TransferEventsHistory = 100
	ci.TransferEventsProgress = fs.Duration(20 * time.Millisecond)
	ctx = WithStatsGroup(ctx, "test-events-progress")
	stats := Stats(ctx)

	in := io.NopCloser(bytes.NewBuffer(make([]byte, 100)))
	tr := stats.NewTransferRemoteSize("big.bin", 100, nil, nil)
	acc := tr.Account(ctx, in)
	acc.AccountReadN(50)

	deadline := time.After(2 * time.Second)
	var progress []TransferEvent
loop:
	for {
		events := stats.Events().Query(EventFilter{}, 0, 0)
		for _, e := range events {
			if e.Type == TransferEventProgress {
				progress = append(progress, e)
				break loop
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for progress event")
		case <-time.After(10 * time.Millisecond):
		}
	}
	require.NotEmpty(t, progress)
	assert.Equal(t, "big.bin", progress[0].Object)
	assert.Equal(t, int64(50), progress[0].Bytes)

	tr.Done(ctx, nil)
}

func TestTransferEventsRCHandler(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)
	group, _ := StatsGroupFromContext(ctx)
	stats.QueuedTransferEvent(ctx, "rc/a.txt", 1, nil, nil, "", "")
	stats.QueuedTransferEvent(ctx, "rc/b.txt", 2, nil, nil, "", "")
	tr := stats.NewTransferRemoteSize("rc/a.txt", 1, nil, nil)
	tr.Done(ctx, nil)

	out, err := rcTransferEvents(ctx, rc.Params{"group": group, "types": "completed,failed"})
	require.NoError(t, err)
	events, ok := out["events"].([]TransferEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	assert.Equal(t, "rc/a.txt", events[0].Object)
	lastSeq, ok := out["lastSeq"].(int64)
	require.True(t, ok)
	assert.GreaterOrEqual(t, lastSeq, events[0].Seq)

	// cursor returns nothing new
	out, err = rcTransferEvents(ctx, rc.Params{"group": group, "since": lastSeq})
	require.NoError(t, err)
	assert.Empty(t, out["events"])

	// path filter as array of types
	out, err = rcTransferEvents(ctx, rc.Params{
		"group": group,
		"path":  "rc/b*",
		"types": []any{"queued"},
	})
	require.NoError(t, err)
	events, _ = out["events"].([]TransferEvent)
	require.Len(t, events, 1)
	assert.Equal(t, "rc/b.txt", events[0].Object)

	// invalid type rejected
	_, err = rcTransferEvents(ctx, rc.Params{"group": group, "types": "bogus"})
	require.Error(t, err)
}

func TestTransferEventsRCHandlerWait(t *testing.T) {
	ctx, stats := newEventTestStats(t, 100)
	group, _ := StatsGroupFromContext(ctx)

	go func() {
		time.Sleep(50 * time.Millisecond)
		stats.QueuedTransferEvent(ctx, "late.txt", 1, nil, nil, "", "")
	}()

	start := time.Now()
	out, err := rcTransferEvents(ctx, rc.Params{
		"group": group,
		"wait":  "1s",
	})
	require.NoError(t, err)
	events, _ := out["events"].([]TransferEvent)
	require.Len(t, events, 1)
	assert.Equal(t, "late.txt", events[0].Object)
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}
