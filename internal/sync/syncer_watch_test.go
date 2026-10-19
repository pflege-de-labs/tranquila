package sync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	"github.com/pflege-de-labs/tranquila/internal/watcher"
	"go.opentelemetry.io/otel/metric/noop"
)

// testMetrics builds no-op instruments so tests can exercise code paths that
// record metrics without a real meter.
func testMetrics(t *testing.T) metrics {
	t.Helper()
	m, err := newMetrics(noop.Meter{})
	if err != nil {
		t.Fatalf("newMetrics: %v", err)
	}
	return m
}

// fastSleep replaces the real backoff sleeper: backoff durations are asserted
// separately, so tests only need cancellation to be honoured.
func fastSleep(ctx context.Context, _ time.Duration) bool {
	return waitOrDone(ctx, time.Millisecond)
}

// fakeWatcher implements watcher.Watcher for testing runWatcher.
type fakeWatcher struct {
	events []watcher.ObjectEvent
	err    error
	calls  atomic.Int32
}

func (f *fakeWatcher) Watch(_ context.Context, _ []string) (<-chan watcher.ObjectEvent, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	ch := make(chan watcher.ObjectEvent, len(f.events))
	for _, e := range f.events {
		ch <- e
	}
	close(ch)
	return ch, nil
}

func TestRunWatch(t *testing.T) {
	tests := []struct {
		name        string
		cycleFn     func(ctx context.Context) error
		interval    time.Duration
		cancelAt    time.Duration // 0 = cancel before starting
		wantErr     bool
		minCycles   int
		minAttempts int
	}{
		{
			name:      "cancel_before_first_cycle",
			cycleFn:   func(ctx context.Context) error { return context.Canceled },
			interval:  time.Hour,
			cancelAt:  0,
			minCycles: 0,
		},
		{
			// Was cycle_error_propagates: a transient failure used to abort the
			// loop and exit the process. It must now back off and retry instead.
			name: "transient_cycle_error_does_not_terminate",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error {
					return errors.New("boom")
				}
			}(),
			interval:    time.Millisecond,
			cancelAt:    time.Second,
			wantErr:     false,
			minAttempts: 2,
		},
		{
			name: "permanent_cycle_error_propagates",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error {
					return &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}
				}
			}(),
			interval: time.Millisecond,
			cancelAt: time.Second,
			wantErr:  true,
		},
		{
			name: "two_cycles_then_cancel",
			cycleFn: func() func(context.Context) error {
				return func(ctx context.Context) error { return nil }
			}(),
			interval:  time.Millisecond,
			cancelAt:  50 * time.Millisecond,
			minCycles: 2,
		},
		{
			name:      "cancel_during_sleep_exits_cleanly",
			cycleFn:   func(ctx context.Context) error { return nil },
			interval:  time.Hour,
			cancelAt:  20 * time.Millisecond,
			minCycles: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Syncer{m: testMetrics(t)}

			var (
				mu        sync.Mutex
				completed int
				attempts  int
			)
			wrapped := func(ctx context.Context) error {
				mu.Lock()
				attempts++
				mu.Unlock()
				err := tc.cycleFn(ctx)
				if err == nil {
					mu.Lock()
					completed++
					mu.Unlock()
				}
				return err
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelAt == 0 {
				cancel()
			} else {
				time.AfterFunc(tc.cancelAt, cancel)
			}

			err := s.runWatch(ctx, tc.interval, wrapped, fastSleep)

			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			mu.Lock()
			n, a := completed, attempts
			mu.Unlock()
			if n < tc.minCycles {
				t.Errorf("completed %d cycles, want at least %d", n, tc.minCycles)
			}
			if a < tc.minAttempts {
				t.Errorf("made %d attempts, want at least %d", a, tc.minAttempts)
			}
		})
	}
}

func TestRunWatcherStartError(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{err: errors.New("watcher init failed")}
	err := s.runWatcher(context.Background(), w, nil, nil)
	if err == nil {
		t.Error("expected error from Watch(), got nil")
	}
}

func TestRunWatcherEmptyChannel(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: nil}
	// Nil bucket map — unknown-bucket events are skipped.
	err := s.runWatcher(context.Background(), w, nil, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunWatcherUnknownBucketSkipped(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: []watcher.ObjectEvent{
		{Bucket: "unknown", Key: "k1", Size: 100},
	}}
	// Empty bucket map — event should be skipped, not panic.
	err := s.runWatcher(context.Background(), w, []string{"unknown"}, map[string]BucketConfig{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunWatcherDeleteEventPropagateDeletesDisabled(t *testing.T) {
	s := &Syncer{}
	w := &fakeWatcher{events: []watcher.ObjectEvent{
		{Bucket: "b1", Key: "k1", IsDelete: true},
	}}
	// PropagateDeletes not set on the bucket config — the delete event must be
	// ignored (never reach pool.submit, which would block forever with the
	// zero-worker pool this test's zero-value Syncer creates).
	err := s.runWatcher(context.Background(), w, []string{"b1"}, map[string]BucketConfig{
		"b1": {Destination: "dst1"},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRunWatcherWithCatchUpDoesNotBlockEventLoop is the regression test for
// watch mode being dead in production: the catch-up sync used to run to
// completion *before* the event loop, so one bucket whose discovery never
// succeeded (attempt=179 and counting) meant no live events were consumed for
// any bucket at all.
func TestRunWatcherWithCatchUpDoesNotBlockEventLoop(t *testing.T) {
	s := &Syncer{m: testMetrics(t)}
	w := &fakeWatcher{} // no events; runWatcher returns and the loop reconnects

	var attempts atomic.Int32
	cycleFn := func(context.Context) error {
		attempts.Add(1)
		return errors.New("bucket events: list objects: context deadline exceeded")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(150*time.Millisecond, cancel)

	err := s.runWatcherWithCatchUp(ctx, w, nil, nil, cycleFn, fastSleep)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if w.calls.Load() == 0 {
		t.Error("event loop never started while the catch-up sync kept failing")
	}
	if attempts.Load() == 0 {
		t.Error("catch-up sync was never attempted")
	}
}

// TestRunWatcherWithCatchUpFatalErrorStopsWatcher pins that a misconfiguration
// (all-permanent cycle error) still terminates instead of leaving the event
// loop running until SIGTERM.
func TestRunWatcherWithCatchUpFatalErrorStopsWatcher(t *testing.T) {
	s := &Syncer{m: testMetrics(t)}
	w := &fakeWatcher{}
	fatal := &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"}

	// Generous safety net: if the fatal path stops cancelling the event loop,
	// this fails on elapsed time rather than hanging the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := s.runWatcherWithCatchUp(ctx, w, nil, nil, func(context.Context) error { return fatal }, fastSleep)
	if !errors.Is(err, fatal) {
		t.Errorf("got err %v, want %v", err, fatal)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v — a fatal catch-up must stop the event loop, not wait for SIGTERM", elapsed)
	}
}

func TestEventDispatch(t *testing.T) {
	tests := []struct {
		name  string
		event watcher.ObjectEvent
		bc    BucketConfig
		want  eventDecision
	}{
		{
			name:  "created_event_is_upload",
			event: watcher.ObjectEvent{IsDelete: false},
			bc:    BucketConfig{PropagateDeletes: true},
			want:  dispatchUpload,
		},
		{
			name:  "delete_event_with_propagate_enabled",
			event: watcher.ObjectEvent{IsDelete: true},
			bc:    BucketConfig{PropagateDeletes: true},
			want:  dispatchDelete,
		},
		{
			name:  "delete_event_with_propagate_disabled",
			event: watcher.ObjectEvent{IsDelete: true},
			bc:    BucketConfig{PropagateDeletes: false},
			want:  dispatchSkipDelete,
		},
		{
			name:  "created_event_inside_prefix_is_upload",
			event: watcher.ObjectEvent{Key: "2026/1/5/a.json"},
			bc:    BucketConfig{SrcPrefix: "2026/1"},
			want:  dispatchUpload,
		},
		{
			name:  "created_event_outside_prefix_is_ignored",
			event: watcher.ObjectEvent{Key: "2026/2/10/fo.json"},
			bc:    BucketConfig{SrcPrefix: "2026/1/"},
			want:  dispatchOutOfPrefix,
		},
		{
			name:  "delete_event_outside_prefix_is_ignored_even_with_propagate",
			event: watcher.ObjectEvent{Key: "other/x", IsDelete: true},
			bc:    BucketConfig{SrcPrefix: "2026/", PropagateDeletes: true},
			want:  dispatchOutOfPrefix,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := eventDispatch(tc.event, tc.bc); got != tc.want {
				t.Errorf("eventDispatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBurnNowForEvent(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		event watcher.ObjectEvent
		bc    BucketConfig
		want  bool
	}{
		{
			name:  "bar_disabled_never_burns",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: false},
			want:  false,
		},
		{
			name:  "bar_enabled_no_gate_burns_immediately",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: true},
			want:  true,
		},
		{
			name:  "bar_enabled_gated_and_old_enough",
			event: watcher.ObjectEvent{ModifiedAt: now.Add(-8 * 24 * time.Hour)},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  true,
		},
		{
			name:  "bar_enabled_gated_too_young_defers",
			event: watcher.ObjectEvent{ModifiedAt: now},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  false,
		},
		{
			name: "bar_enabled_gated_unknown_modified_at_defers",
			// ObjectEvent.ModifiedAt is documented best-effort — may be zero.
			event: watcher.ObjectEvent{},
			bc:    BucketConfig{BurnAfterReading: true, BurnAfterReadingMinAge: 7 * 24 * time.Hour},
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := burnNowForEvent(tc.event, tc.bc, now); got != tc.want {
				t.Errorf("burnNowForEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}
