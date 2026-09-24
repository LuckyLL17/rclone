package accounting

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/rc"
)

// TransferEventType identifies one point in a file's transfer lifecycle.
type TransferEventType string

// Transfer event types, in the order a file normally moves through the
// transfer pipeline:
//
// queued -> started -> progress* -> retried* -> completed
//
//	\-> failed
//
// queued ---------------------------> skipped
const (
	// TransferEventQueued is emitted when a file is added to the
	// transfer queue after comparison decided it needs transferring.
	TransferEventQueued TransferEventType = "queued"
	// TransferEventStarted is emitted when a worker starts processing
	// the file (before any bytes are read).
	TransferEventStarted TransferEventType = "started"
	// TransferEventProgress is emitted periodically while bytes of the
	// file are being transferred.
	TransferEventProgress TransferEventType = "progress"
	// TransferEventRetried is emitted when a low level copy attempt
	// failed and the transfer is retried.
	TransferEventRetried TransferEventType = "retried"
	// TransferEventCompleted is emitted when the file was transferred
	// successfully.
	TransferEventCompleted TransferEventType = "completed"
	// TransferEventSkipped is emitted when an up to date file was left
	// in place without transferring it.
	TransferEventSkipped TransferEventType = "skipped"
	// TransferEventFailed is emitted when a transfer or check failed,
	// including when the owning context was cancelled while the file
	// was queued or in flight.
	TransferEventFailed TransferEventType = "failed"
)

// AllTransferEventTypes is the complete set of transfer event types in
// lifecycle order.
var AllTransferEventTypes = []TransferEventType{
	TransferEventQueued,
	TransferEventStarted,
	TransferEventProgress,
	TransferEventRetried,
	TransferEventCompleted,
	TransferEventSkipped,
	TransferEventFailed,
}

// TransferEvent describes a single lifecycle transition of one object.
//
// Seq is a process-wide monotonically increasing identifier assigned to
// every event regardless of stats group; it is stable once emitted and
// can be used as a replay cursor.
type TransferEvent struct {
	Seq      int64             `json:"seq"`
	Time     time.Time         `json:"time"`
	Group    string            `json:"group,omitempty"`
	JobID    int64             `json:"jobid,omitempty"`
	Type     TransferEventType `json:"event"`
	Object   string            `json:"object"`
	Size     int64             `json:"size"`
	Bytes    int64             `json:"bytes,omitempty"`
	HashType string            `json:"hashType,omitempty"`
	Hash     string            `json:"hash,omitempty"`
	Attempt  int               `json:"attempt,omitempty"`
	Error    string            `json:"error,omitempty"`
	SrcFs    string            `json:"srcFs,omitempty"`
	DstFs    string            `json:"dstFs,omitempty"`
}

// globalEventSeq assigns process-wide stable sequence numbers to events
// from every stats group.
var globalEventSeq atomic.Int64

// LastTransferEventSeq returns the highest event sequence number
// assigned so far across all stats groups.
func LastTransferEventSeq() int64 {
	return globalEventSeq.Load()
}

// eventSubscription is a live event delivery channel that is never
// closed; cancellation is signalled through done.
type eventSubscription struct {
	ch   chan TransferEvent
	done chan struct{}
}

// EventStream keeps the transfer events of one stats group (a CLI run
// or an RC job/group): a bounded history ring for replay, an in-flight
// index used to enrich and cancel events, and live subscribers.
type EventStream struct {
	mu         sync.Mutex
	maxHistory int
	history    []TransferEvent // ring buffer; valid entries walk from head, count long
	head       int             // index of the oldest event once the ring is full
	count      int             // number of events stored in history
	inFlight   map[string]TransferEvent
	pending    map[string]TransferEvent // queued events not yet started, carrying the hash forward
	subs       map[int64]eventSubscription
	nextSubID  int64
	wake       chan struct{} // closed on every change, then replaced
}

func newEventStream(maxHistory int) *EventStream {
	return &EventStream{
		maxHistory: maxHistory,
		inFlight:   make(map[string]TransferEvent),
		pending:    make(map[string]TransferEvent),
		subs:       make(map[int64]eventSubscription),
		wake:       make(chan struct{}),
	}
}

// remember stores e in the bounded ring buffer.
//
// Must be called with es.mu held.
func (es *EventStream) remember(e TransferEvent) {
	if es.maxHistory <= 0 {
		return
	}
	if es.count < len(es.history) {
		es.history[es.count] = e
		es.count++
		return
	}
	if es.count < es.maxHistory {
		es.history = append(es.history, e)
		es.count++
		return
	}
	es.history[es.head] = e
	es.head = (es.head + 1) % len(es.history)
}

// ordered returns the stored history oldest first.
//
// Must be called with es.mu held.
func (es *EventStream) ordered() []TransferEvent {
	if es.count == 0 {
		return nil
	}
	n := len(es.history)
	out := make([]TransferEvent, 0, es.count)
	end := min(es.head+es.count, n)
	out = append(out, es.history[es.head:end]...)
	if wrapped := es.head + es.count - n; wrapped > 0 {
		out = append(out, es.history[:wrapped]...)
	}
	return out
}

// emit assigns sequence number and time to e, stores it, updates the
// in-flight index and fans the event out to live subscribers.
func (es *EventStream) emit(ctx context.Context, group string, e TransferEvent) {
	e.Seq = globalEventSeq.Add(1)
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	if e.Group == "" {
		e.Group = group
	}
	if e.JobID == 0 && cache.JobGetJobID != nil {
		if jobID, ok := cache.JobGetJobID(ctx); ok {
			e.JobID = jobID
		}
	}

	var deliveries []eventSubscription
	var wake chan struct{}

	es.mu.Lock()
	if e.Hash == "" {
		if queued, ok := es.pending[e.Object]; ok && queued.Hash != "" {
			e.HashType, e.Hash = queued.HashType, queued.Hash
		} else if active, ok := es.inFlight[e.Object]; ok && active.Hash != "" {
			e.HashType, e.Hash = active.HashType, active.Hash
		}
	}
	es.remember(e)
	switch e.Type {
	case TransferEventQueued:
		es.pending[e.Object] = e
		es.inFlight[e.Object] = e
	case TransferEventStarted, TransferEventProgress, TransferEventRetried:
		delete(es.pending, e.Object)
		es.inFlight[e.Object] = e
	case TransferEventCompleted, TransferEventSkipped, TransferEventFailed:
		delete(es.pending, e.Object)
		delete(es.inFlight, e.Object)
	}
	wake = es.wake
	es.wake = make(chan struct{})
	for _, sub := range es.subs {
		deliveries = append(deliveries, sub)
	}
	es.mu.Unlock()

	close(wake)
	for _, sub := range deliveries {
		select {
		case sub.ch <- e:
		case <-sub.done:
		default: // drop the event for a slow subscriber rather than stalling transfers
		}
	}
}

// Query returns stored events with seq greater than since matching f,
// oldest first, up to limit events (limit <= 0 means unlimited).
func (es *EventStream) Query(f EventFilter, since int64, limit int) []TransferEvent {
	es.mu.Lock()
	events := es.ordered()
	es.mu.Unlock()

	var out []TransferEvent
	for _, e := range events {
		if e.Seq <= since || !f.match(e) {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// Subscribe registers a live subscriber receiving events emitted after
// subscription. It returns the events channel, a channel closed by
// cancel, and the cancel function.
func (es *EventStream) Subscribe() (events <-chan TransferEvent, done <-chan struct{}, cancel func()) {
	sub := eventSubscription{
		ch:   make(chan TransferEvent, 256),
		done: make(chan struct{}),
	}
	es.mu.Lock()
	id := es.nextSubID
	es.nextSubID++
	es.subs[id] = sub
	es.mu.Unlock()

	var once sync.Once
	cancel = func() {
		once.Do(func() {
			close(sub.done)
			es.mu.Lock()
			delete(es.subs, id)
			es.mu.Unlock()
		})
	}
	return sub.ch, sub.done, cancel
}

// wakeChan returns a channel closed when the stream next changes.
func (es *EventStream) wakeChan() <-chan struct{} {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.wake
}

// Finish marks the end of a pass over this stream: all objects still
// queued or in flight are emitted as failed events when err is non nil
// (e.g. a cancelled context), the in-flight index is cleared and live
// subscribers / long polls are woken.
//
// It is safe to call multiple times, for example once per sync pass
// sharing a stats group.
func (es *EventStream) Finish(ctx context.Context, err error) {
	var leftover []TransferEvent
	es.mu.Lock()
	if err != nil {
		for _, e := range es.inFlight {
			e.Type = TransferEventFailed
			e.Error = err.Error()
			e.Time = time.Time{}
			leftover = append(leftover, e)
		}
	}
	es.inFlight = make(map[string]TransferEvent)
	es.pending = make(map[string]TransferEvent)
	wake := es.wake
	es.wake = make(chan struct{})
	es.mu.Unlock()

	close(wake)

	sort.Slice(leftover, func(i, j int) bool {
		return leftover[i].Seq < leftover[j].Seq
	})
	for i := range leftover {
		es.emit(ctx, "", leftover[i])
	}
}

// Reset discards all history and in-flight state.
func (es *EventStream) Reset() {
	es.mu.Lock()
	es.history = nil
	es.head = 0
	es.count = 0
	es.inFlight = make(map[string]TransferEvent)
	es.pending = make(map[string]TransferEvent)
	wake := es.wake
	es.wake = make(chan struct{})
	es.mu.Unlock()
	close(wake)
}

// EventFilter selects transfer events by object path glob and event
// type.
type EventFilter struct {
	pathRe *regexp.Regexp
	types  map[TransferEventType]struct{}
}

// NewEventFilter builds an EventFilter from an rclone glob pattern
// (empty matches all paths) and a list of event types (empty matches
// all types).
func NewEventFilter(pathPattern string, types []string) (f EventFilter, err error) {
	if pathPattern != "" {
		f.pathRe, err = filter.GlobPathToRegexp(pathPattern, false)
		if err != nil {
			return f, fmt.Errorf("invalid path pattern %q: %w", pathPattern, err)
		}
	}
	if len(types) > 0 {
		f.types = make(map[TransferEventType]struct{}, len(types))
		for _, typ := range types {
			t := TransferEventType(strings.TrimSpace(typ))
			if !slices.Contains(AllTransferEventTypes, t) {
				return f, fmt.Errorf("unknown event type %q", t)
			}
			f.types[t] = struct{}{}
		}
	}
	return f, nil
}

// Match reports whether e passes the filter.
func (f EventFilter) match(e TransferEvent) bool {
	if f.pathRe != nil && !f.pathRe.MatchString(e.Object) {
		return false
	}
	if f.types != nil {
		_, ok := f.types[e.Type]
		return ok
	}
	return true
}

// Events returns the transfer event stream for these stats, or nil if
// transfer events are not enabled.
func (s *StatsInfo) Events() *EventStream {
	return s.events
}

// SubscribeTransferEvents subscribes to live events for these stats,
// returning the events channel, a channel closed by cancel, and the
// cancel function. When events are disabled events is nil and cancel
// is a no-op.
func (s *StatsInfo) SubscribeTransferEvents() (events <-chan TransferEvent, done <-chan struct{}, cancel func()) {
	if es := s.events; es != nil {
		return es.Subscribe()
	}
	return nil, nil, func() {}
}

// emitTransferEvent records an event on this stats group.
func (s *StatsInfo) emitTransferEvent(ctx context.Context, e TransferEvent) {
	es := s.events
	if es == nil {
		return
	}
	if e.JobID == 0 {
		e.JobID = s.jobID
	}
	es.emit(ctx, s.group, e)
}

func fsString(f fs.Fs) string {
	if f == nil {
		return ""
	}
	return fs.ConfigString(f)
}

// QueuedTransferEvent records a queued event for remote.
func (s *StatsInfo) QueuedTransferEvent(ctx context.Context, remote string, size int64, srcFs, dstFs fs.Fs, hashType, hashSum string) {
	s.emitTransferEvent(ctx, TransferEvent{
		Type:     TransferEventQueued,
		Object:   remote,
		Size:     size,
		HashType: hashType,
		Hash:     hashSum,
		SrcFs:    fsString(srcFs),
		DstFs:    fsString(dstFs),
	})
}

// SkippedTransferEvent records a skipped event for remote.
func (s *StatsInfo) SkippedTransferEvent(ctx context.Context, remote string, size int64, srcFs, dstFs fs.Fs, hashType, hashSum string) {
	s.emitTransferEvent(ctx, TransferEvent{
		Type:     TransferEventSkipped,
		Object:   remote,
		Size:     size,
		HashType: hashType,
		Hash:     hashSum,
		SrcFs:    fsString(srcFs),
		DstFs:    fsString(dstFs),
	})
}

// progressEvent records a progress event for an in-flight transfer.
func (s *StatsInfo) progressEvent(ctx context.Context, remote string, size, bytes int64) {
	s.emitTransferEvent(ctx, TransferEvent{
		Type:   TransferEventProgress,
		Object: remote,
		Size:   size,
		Bytes:  bytes,
	})
}

// StartedTransferEvent records a started event.
func (s *StatsInfo) StartedTransferEvent(remote string, size int64, srcFs, dstFs fs.Fs) {
	s.emitTransferEvent(context.Background(), TransferEvent{
		Type:   TransferEventStarted,
		Object: remote,
		Size:   size,
		SrcFs:  fsString(srcFs),
		DstFs:  fsString(dstFs),
	})
}

// FailedTransferEvent records a failed event.
func (s *StatsInfo) FailedTransferEvent(ctx context.Context, remote string, size int64, srcFs, dstFs fs.Fs, attempt int, err error) {
	s.emitTransferEvent(ctx, TransferEvent{
		Type:    TransferEventFailed,
		Object:  remote,
		Size:    size,
		Attempt: attempt,
		Error:   err.Error(),
		SrcFs:   fsString(srcFs),
		DstFs:   fsString(dstFs),
	})
}

// EventsFinish marks the end of a pass over this stats group's
// transfers, turning files abandoned mid-lifecycle because of err into
// failed events. It is a no-op when transfer events are disabled.
func (s *StatsInfo) EventsFinish(ctx context.Context, err error) {
	if es := s.events; es != nil {
		es.Finish(ctx, err)
	}
}

// allEventStreams returns the event streams of every stats group.
func allEventStreams() []*EventStream {
	groups.mu.Lock()
	defer groups.mu.Unlock()
	streams := make([]*EventStream, 0, len(groups.m))
	for _, stats := range groups.m {
		if es := stats.Events(); es != nil {
			streams = append(streams, es)
		}
	}
	return streams
}

// parseEventTypeList accepts a comma separated string or a list of
// event type names.
func parseEventTypeList(v any) ([]string, error) {
	switch x := v.(type) {
	case string:
		var out []string
		for _, typ := range strings.Split(x, ",") {
			if typ = strings.TrimSpace(typ); typ != "" {
				out = append(out, typ)
			}
		}
		return out, nil
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			typ, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("event types must be strings")
			}
			out = append(out, typ)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("event types must be a string or list of strings")
	}
}

func init() {
	rc.Add(rc.Call{
		Path:   "core/transfer-events",
		NoAuth: true,
		Fn:     rcTransferEvents,
		Title:  "Query the structured per-file transfer event stream",
		Help: `
This returns lifecycle events for file transfers:

	rclone rc core/transfer-events

Events have types queued, started, progress, retried, completed,
skipped and failed, each carrying a stable monotonic "seq", the object
path, size, hash (when known), timestamps and the stats group / job id.

Only events produced while --transfer-events was enabled (or while the
group configuration had transfer-events set) are kept, and only the
last --transfer-events-history events are available per group.

Enable it for an rc job with a flat "transfer_events": true parameter
or with _config using the Go name, e.g.

	rclone rc sync/copy --json '{"srcFs":"/src","dstFs":"/dst","_async":true,"transfer_events":true}'

Parameters

- group - name of the stats group, e.g. "job/1"; if omitted events from
  every group are returned (string, optional)
- path - rclone glob pattern matched against the object path, e.g.
  "dir/*.jpg" (string, optional)
- types - comma separated list or array of event types to return,
  e.g. "completed,failed" (string or array, optional)
- since - only return events with a seq greater than this (integer,
  optional; use the lastSeq of the previous response as a cursor)
- limit - maximum number of events to return (integer, default 100,
  max 10000)
- wait - if no matching events are available, wait up to this long for
  new events to arrive before returning (duration, e.g. "30s", optional)

Results:

` + "```" + `
{
	"events": [
		{
			"seq": 42,
			"time": "2024-01-02T15:04:05.123456789Z",
			"group": "job/1",
			"jobid": 1,
			"event": "completed",
			"object": "path/to/file.txt",
			size": 1234,
			"bytes": 1234,
			"hashType": "md5",
			"hash": "....",
			"attempt": 1,
			"error": "",
			"srcFs": "local:/src",
			"dstFs": "drive:dst"
		}
	],
	"lastSeq": 42
}
` + "```" + `

Cancelling a job with job/stop cancels its context: queued and
in-flight objects then receive failed events with the context error,
which are returned by this call in the same way as any other failure.
`,
	})
}

const (
	defaultTransferEventLimit = 100
	maxTransferEventLimit     = 10000
)

// rcTransferEvents implements the core/transfer-events RC call.
func rcTransferEvents(ctx context.Context, in rc.Params) (rc.Params, error) {
	group, err := in.GetString("group")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}

	var types []string
	if typesRaw, err := in.Get("types"); err == nil {
		types, err = parseEventTypeList(typesRaw)
		if err != nil {
			return nil, rc.NewErrParamInvalid(err)
		}
	} else if !rc.IsErrParamNotFound(err) {
		return nil, err
	}

	pathPattern, err := in.GetString("path")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}

	since, err := in.GetInt64("since")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}

	limit := defaultTransferEventLimit
	if limitRaw, err := in.GetInt("limit"); err == nil {
		limit = limitRaw
	} else if !rc.IsErrParamNotFound(err) {
		return nil, err
	}
	if limit < 0 {
		limit = 0
	} else if limit == 0 || limit > maxTransferEventLimit {
		limit = maxTransferEventLimit
	}

	wait, err := in.GetDuration("wait")
	if rc.NotErrParamNotFound(err) {
		return nil, err
	}

	f, err := NewEventFilter(pathPattern, types)
	if err != nil {
		return nil, rc.NewErrParamInvalid(err)
	}

	var streams []*EventStream
	if group != "" {
		if es := StatsGroup(ctx, group).Events(); es != nil {
			streams = append(streams, es)
		}
	} else {
		streams = allEventStreams()
	}

	query := func() []TransferEvent {
		var events []TransferEvent
		for _, es := range streams {
			events = append(events, es.Query(f, since, 0)...)
		}
		sort.Slice(events, func(i, j int) bool {
			return events[i].Seq < events[j].Seq
		})
		if limit > 0 && len(events) > limit {
			events = events[:limit]
		}
		return events
	}

	events := query()
	if len(events) == 0 && wait > 0 && len(streams) > 0 {
		waitCtx, cancelWait := context.WithTimeout(ctx, wait)
		woken := make(chan struct{})
		var wakeWG sync.WaitGroup
		wakeWG.Add(len(streams))
		for _, es := range streams {
			ch := es.wakeChan()
			go func() {
				defer wakeWG.Done()
				select {
				case <-ch:
				case <-waitCtx.Done():
				}
			}()
		}
		go func() {
			wakeWG.Wait()
			close(woken)
		}()

		select {
		case <-woken:
		case <-ctx.Done():
		}
		cancelWait()
		events = query()
	}

	if events == nil {
		events = []TransferEvent{}
	}
	out := rc.Params{
		"events":  events,
		"lastSeq": LastTransferEventSeq(),
	}
	return out, nil
}
