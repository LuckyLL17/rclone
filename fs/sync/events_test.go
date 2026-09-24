package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fstest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fstestWaitTimeout  = 10 * time.Second
	fstestWaitInterval = 10 * time.Millisecond
)

// eventsByObject groups events by object path in emission order.
func eventsByObject(events []accounting.TransferEvent) map[string][]accounting.TransferEventType {
	out := map[string][]accounting.TransferEventType{}
	for _, e := range events {
		out[e.Object] = append(out[e.Object], e.Type)
	}
	return out
}

// TestSyncTransferEvents checks the full lifecycle events of a copy
// run with one up to date (skipped) and one new (queued, started,
// completed) file.
func TestSyncTransferEvents(t *testing.T) {
	// Seed the destination with an up to date copy of one.txt using
	// an ungrouped context so the test stats group is only created
	// once events are enabled.
	seedCtx := context.Background()
	r := fstest.NewRun(t)
	file1 := r.WriteFile("one.txt", "hello", t1)
	file2 := r.WriteFile("sub dir/two.txt", "world!", t2)
	r.Mkdir(seedCtx, r.Fremote)
	require.NoError(t, operations.CopyFile(seedCtx, r.Fremote, r.Flocal, "one.txt", "one.txt"))

	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferEvents = true
	ci.TransferEventsHistory = 1000
	const group = "test-sync-transfer-events"
	ctx = accounting.WithStatsGroup(ctx, group)

	require.NoError(t, CopyDir(ctx, r.Fremote, r.Flocal, false))

	r.CheckRemoteItems(t, file1, file2)

	stats := accounting.Stats(ctx)
	es := stats.Events()
	require.NotNil(t, es)
	events := es.Query(accounting.EventFilter{}, 0, 0)
	require.NotEmpty(t, events)

	// sequence numbers are monotonic and every event belongs to the group
	var lastSeq int64
	for _, e := range events {
		assert.Greater(t, e.Seq, lastSeq)
		lastSeq = e.Seq
		assert.Equal(t, group, e.Group)
	}

	byObject := eventsByObject(events)
	assert.Equal(t, []accounting.TransferEventType{accounting.TransferEventSkipped},
		byObject["one.txt"])
	assert.Equal(t, []accounting.TransferEventType{
		accounting.TransferEventQueued,
		accounting.TransferEventStarted,
		accounting.TransferEventCompleted,
	}, byObject["sub dir/two.txt"])

	// the transfer events carry path, size and hash information
	var completed accounting.TransferEvent
	for _, e := range events {
		if e.Object == "sub dir/two.txt" && e.Type == accounting.TransferEventCompleted {
			completed = e
		}
	}
	assert.Equal(t, int64(len("world!")), completed.Size)
	assert.NotEmpty(t, completed.Hash)

	// filters work against the replay history
	f, err := accounting.NewEventFilter("sub dir/*", []string{"completed"})
	require.NoError(t, err)
	filtered := es.Query(f, 0, 0)
	require.Len(t, filtered, 1)
	assert.Equal(t, "sub dir/two.txt", filtered[0].Object)

	// cursor replay returns nothing after the last event
	assert.Empty(t, es.Query(accounting.EventFilter{}, lastSeq, 0))
}

// TestSyncTransferEventsCancelled checks that files still queued when
// the context is cancelled receive failed events.
func TestSyncTransferEventsCancelled(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.TransferEvents = true
	ci.TransferEventsHistory = 1000
	ci.Transfers = 1
	ci.Checkers = 1
	ctx = accounting.WithStatsGroup(ctx, "test-sync-transfer-events-cancelled")

	r := fstest.NewRun(t)
	r.Mkdir(context.Background(), r.Fremote)
	// Several files; the run is cancelled before it can finish
	for i := range 20 {
		r.WriteFile(fmt.Sprintf("file%02d.txt", i), "some file contents", t1)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = CopyDir(runCtx, r.Fremote, r.Flocal, false)
	}()

	stats := accounting.Stats(ctx)

	// Wait until every file has entered the transfer queue before
	// cancelling so the cancel/failed semantics are deterministic.
	queuedOnly, err := accounting.NewEventFilter("", []string{"queued"})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(stats.Events().Query(queuedOnly, 0, 0)) == 20
	}, fstestWaitTimeout, fstestWaitInterval)
	cancel()

	// Every file that did not complete or get skipped ends with a
	// failed event from the cancelled context
	require.Eventually(t, func() bool {
		events := stats.Events().Query(accounting.EventFilter{}, 0, 0)
		byObject := eventsByObject(events)
		for i := range 20 {
			types := byObject[fmt.Sprintf("file%02d.txt", i)]
			var terminal accounting.TransferEventType
			if len(types) > 0 {
				terminal = types[len(types)-1]
			}
			if terminal != accounting.TransferEventCompleted &&
				terminal != accounting.TransferEventSkipped &&
				terminal != accounting.TransferEventFailed {
				return false
			}
		}
		return true
	}, fstestWaitTimeout, fstestWaitInterval)

	select {
	case <-runDone:
	case <-time.After(fstestWaitTimeout):
		t.Fatal("cancelled sync did not stop")
	}
}
