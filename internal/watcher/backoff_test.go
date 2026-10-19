package watcher

import (
	"context"
	"testing"
	"time"
)

// noRetry stops a retry loop at its first backoff.
func noRetry(context.Context, time.Duration) bool { return false }

// recordingSleeper records each requested delay and stops after limit calls.
type recordingSleeper struct {
	delays []time.Duration
	limit  int
}

func (r *recordingSleeper) sleep(_ context.Context, d time.Duration) bool {
	r.delays = append(r.delays, d)
	return len(r.delays) < r.limit
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		n        int
		min, max time.Duration
	}{
		{n: 1, min: retryBase, max: 2 * retryBase},
		{n: 2, min: retryBase, max: 3 * retryBase},
		{n: 4, min: retryBase, max: 9 * retryBase},
		{n: 50, min: retryBase, max: retryBase + retryMax},
	}
	for _, tt := range tests {
		for range 100 {
			if d := retryDelay(tt.n); d < tt.min || d >= tt.max {
				t.Fatalf("retryDelay(%d) = %v, want [%v, %v)", tt.n, d, tt.min, tt.max)
			}
		}
	}
}

func TestWaitOrDone(t *testing.T) {
	if !waitOrDone(context.Background(), time.Millisecond) {
		t.Error("waitOrDone on live ctx = false, want true")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitOrDone(ctx, time.Hour) {
		t.Error("waitOrDone on cancelled ctx = true, want false")
	}
}
