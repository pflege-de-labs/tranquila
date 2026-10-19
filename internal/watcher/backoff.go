package watcher

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	retryBase = time.Second
	retryMax  = 5 * time.Minute
)

// sleeper waits for d, reporting false when ctx was cancelled first.
// Injectable so retry tests need no real delays.
type sleeper func(ctx context.Context, d time.Duration) bool

func waitOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// retryDelay mirrors sync.cycleBackoff (not importable: sync imports watcher):
// base + uniform[0, min(base*2^(n-1), max)) for the nth consecutive failure.
func retryDelay(n int) time.Duration {
	d := retryBase
	for range max(n-1, 0) {
		if d >= retryMax/2 {
			d = retryMax
			break
		}
		d *= 2
	}
	return retryBase + rand.N(d)
}
