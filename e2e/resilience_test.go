package e2e

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pflege-de-labs/tranquila/internal/state"
	"github.com/pflege-de-labs/tranquila/internal/storage"
	tsync "github.com/pflege-de-labs/tranquila/internal/sync"
)

// syncerFor builds a Syncer wired to the given clients with test-scale backoff.
func syncerFor(t *testing.T, s *stack, src, dst *storage.Client, srcBucket, dstBucket string) *tsync.Syncer {
	t.Helper()
	syncer, err := tsync.New(tsync.Config{
		Source:      src,
		Destination: dst,
		State:       s.store(t),
		// Meter is left unset: it is optional and falls back to no-op instruments.
		Buckets: map[string]tsync.BucketConfig{srcBucket: {Destination: dstBucket}},
		Workers: 4,
		// Keep retry pacing test-scale; production defaults are 5s/10m.
		CycleBackoff:    250 * time.Millisecond,
		CycleBackoffMax: time.Second,
	})
	if err != nil {
		t.Fatalf("create syncer: %v", err)
	}
	return syncer
}

// TestSyncCompletesDespiteTransient504 is the regression test for the reported
// production incident: a gateway returning 504 during discovery used to abort
// the run. The retry budget must now absorb it and sync every object.
func TestSyncCompletesDespiteTransient504(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "burst-src", 5)

	srcProxy := newFaultProxy(t, s.s3Endpoint)
	src := s.client(t, "source", srcProxy.URL(), 0, 0)
	dst := s.client(t, "destination", s.s3Endpoint, 0, 0)

	// Fail the opening requests of the run, within the retry budget.
	srcProxy.failNext(3, http.StatusGatewayTimeout, true)

	if err := syncerFor(t, s, src, dst, "burst-src", "burst-dst").Run(ctx); err != nil {
		t.Fatalf("Run should have absorbed the 504 burst: %v", err)
	}
	if _, failed := srcProxy.stats(); failed == 0 {
		t.Error("no faults were injected — the test proved nothing")
	}
	if got := s.countObjects(t, "burst-dst", keys); got != len(keys) {
		t.Errorf("synced %d/%d objects", got, len(keys))
	}
}

// TestWatchSurvivesSustainedOutage covers the guarantee that watch mode never
// exits on a transient fault. Previously one failed cycle returned, reached
// log.Fatal and restarted the pod.
//
// The destination is faulted because EnsureBucket runs first in a cycle and has
// no repo-level retry, so cycles fail in seconds rather than minutes.
func TestWatchSurvivesSustainedOutage(t *testing.T) {
	s := newStack(t)
	keys := s.seed(t, "outage-src", 3)

	dstProxy := newFaultProxy(t, s.s3Endpoint)
	src := s.client(t, "source", s.s3Endpoint, 0, 0)
	dst := s.client(t, "destination", dstProxy.URL(), 0, 0)
	syncer := syncerFor(t, s, src, dst, "outage-src", "outage-dst")

	dstProxy.failAll(http.StatusGatewayTimeout, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- syncer.RunWatch(ctx, 200*time.Millisecond) }()

	// The endpoint is hard down: the loop must keep retrying, not return.
	eventually(t, 90*time.Second, "cycles to start failing", func() bool {
		_, failed := dstProxy.stats()
		return failed > 0
	})
	select {
	case err := <-done:
		t.Fatalf("RunWatch returned during a transient outage: %v", err)
	case <-time.After(2 * time.Second):
		// Still running, which is the point.
	}

	// Recovery: once the endpoint is healthy the next cycle must sync.
	dstProxy.clear()
	eventually(t, 120*time.Second, "objects to sync after recovery", func() bool {
		return s.countObjects(t, "outage-dst", keys) == len(keys)
	})

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("RunWatch returned %v, want nil on cancellation", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("RunWatch did not return after cancellation")
	}
}

// TestWatchTerminatesOnPermanentError is the other half of that guarantee:
// misconfiguration must stay loud rather than retry forever.
func TestWatchTerminatesOnPermanentError(t *testing.T) {
	s := newStack(t)
	s.seed(t, "perm-src", 1)

	dstProxy := newFaultProxy(t, s.s3Endpoint)
	src := s.client(t, "source", s.s3Endpoint, 0, 0)
	dst := s.client(t, "destination", dstProxy.URL(), 0, 0)
	syncer := syncerFor(t, s, src, dst, "perm-src", "perm-dst")

	dstProxy.failAll(http.StatusForbidden, true) // AccessDenied

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- syncer.RunWatch(ctx, 200*time.Millisecond) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("RunWatch returned nil on a permanent error, want it to surface")
		}
	case <-ctx.Done():
		t.Fatal("RunWatch kept retrying a permanent error instead of returning")
	}
}

// TestRateLimitDegradesAndRecovers covers congestion control on each endpoint
// independently, including that an unconfigured endpoint is never degraded.
func TestRateLimitDegradesAndRecovers(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "aimd-src", 1)

	tests := []struct {
		name        string
		rateLimit   float64
		wantDegrade bool
		wantLimit   float64
	}{
		{name: "configured_endpoint_degrades", rateLimit: 40, wantDegrade: true, wantLimit: 20},
		// No ceiling was configured, so there is nothing to halve.
		{name: "unlimited_endpoint_never_degrades", rateLimit: 0, wantDegrade: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := newFaultProxy(t, s.s3Endpoint)
			c := s.client(t, "source", fp.URL(), tt.rateLimit, 2)

			fp.failAll(http.StatusGatewayTimeout, true)
			// Two failing calls meet the threshold of 2.
			for range 2 {
				if _, _, _, err := c.HeadObject(ctx, "aimd-src", keys[0]); err == nil {
					t.Fatal("expected HeadObject to fail while the endpoint returns 504")
				}
			}

			st := c.LimitState()
			if st.Degraded != tt.wantDegrade {
				t.Fatalf("degraded = %v, want %v (state %+v)", st.Degraded, tt.wantDegrade, st)
			}
			if !tt.wantDegrade {
				return
			}
			if st.Current != tt.wantLimit {
				t.Errorf("rate limit = %v, want %v", st.Current, tt.wantLimit)
			}

			// Recovery is additive: 10% of base per 20 healthy calls.
			fp.clear()
			eventually(t, 90*time.Second, "the rate limit to climb back to base", func() bool {
				if _, _, _, err := c.HeadObject(ctx, "aimd-src", keys[0]); err != nil {
					return false
				}
				return !c.LimitState().Degraded
			})
		})
	}
}

// TestDestinationDegradesIndependently pins that a sick destination throttles
// destination writes without slowing source reads.
func TestDestinationDegradesIndependently(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "indep-src", 1)

	dstProxy := newFaultProxy(t, s.s3Endpoint)
	src := s.client(t, "source", s.s3Endpoint, 100, 2)
	dst := s.client(t, "destination", dstProxy.URL(), 40, 2)

	dstProxy.failAll(http.StatusGatewayTimeout, true)
	for range 2 {
		if _, _, _, err := dst.HeadObject(ctx, "indep-src", keys[0]); err == nil {
			t.Fatal("expected the destination call to fail")
		}
	}
	// Keep the source busy and healthy throughout.
	for range 3 {
		if _, _, _, err := src.HeadObject(ctx, "indep-src", keys[0]); err != nil {
			t.Fatalf("source call failed unexpectedly: %v", err)
		}
	}

	if dstSt := dst.LimitState(); !dstSt.Degraded || dstSt.Current != 20 {
		t.Errorf("destination state = %+v, want degraded at 20", dstSt)
	}
	if srcSt := src.LimitState(); srcSt.Degraded || srcSt.Current != 100 {
		t.Errorf("source state = %+v, want untouched at 100", srcSt)
	}
}

// TestL4FaultsAreTransient covers the fault class Toxiproxy can produce that an
// L7 injector cannot: a connection reset mid-stream, at the TCP layer.
func TestL4FaultsAreTransient(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "l4-src", 2)

	c := s.client(t, "source", s.toxicEndpoint, 0, 0)

	if _, err := s.toxics.AddToxic("reset", "reset_peer", "downstream", 1.0, map[string]any{
		"timeout": 0,
	}); err != nil {
		t.Fatalf("add reset_peer toxic: %v", err)
	}

	_, _, _, err := c.HeadObject(ctx, "l4-src", keys[0])
	if err == nil {
		t.Fatal("expected a connection reset to surface as an error")
	}
	// A reset must be retryable, not mistaken for misconfiguration.
	if got := storage.Classify(err); got != storage.ClassTransient {
		t.Errorf("Classify(reset_peer) = %v, want ClassTransient\nerr: %v", got, err)
	}

	if err := s.toxics.RemoveToxic("reset"); err != nil {
		t.Fatalf("remove toxic: %v", err)
	}
	eventually(t, 30*time.Second, "the endpoint to recover after the reset", func() bool {
		_, _, _, err := c.HeadObject(ctx, "l4-src", keys[0])
		return err == nil
	})
}

// markAlreadySynced records key as synced exactly the way a real sync run does
// (status=synced, modified_at=the source object's real mtime), so needsSync
// takes the verify-and-delete path instead of re-uploading. MarkSynced alone
// leaves modified_at unset, which needsSync reads as "source changed since
// last sync" and forces a re-upload instead.
func markAlreadySynced(t *testing.T, ctx context.Context, store *state.Store, src *storage.Client, bucket, key string) {
	t.Helper()
	var objs []storage.Object
	_, _, err := src.ListObjectsPage(ctx, bucket, "", nil, 100, func(page []storage.Object) error {
		objs = append(objs, page...)
		return nil
	})
	if err != nil {
		t.Fatalf("list source objects: %v", err)
	}
	idx := slices.IndexFunc(objs, func(o storage.Object) bool { return o.Key == key })
	if idx < 0 {
		t.Fatalf("source object %s/%s not found", bucket, key)
	}
	err = store.UpsertObject(ctx, bucket, key, state.ObjectState{
		Status:     state.StatusSynced,
		ModifiedAt: objs[idx].ModifiedAt,
		SyncedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("upsert synced state: %v", err)
	}
}

// TestBurnAfterReadingVerifiesContentNotJustSize covers the verify-and-delete
// path: an object already synced before burn-after-reading was enabled must be
// checksum-verified against the source before its source copy is deleted, not
// merely size-checked. A destination silently corrupted or overwritten with
// same-size content must block the delete.
func TestBurnAfterReadingVerifiesContentNotJustSize(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	const bucket = "bar-src"
	const dstBucket = "bar-dst"

	src := s.client(t, "source", s.s3Endpoint, 0, 0)
	dst := s.client(t, "destination", s.s3Endpoint, 0, 0)

	if err := src.EnsureBucket(ctx, bucket); err != nil {
		t.Fatalf("ensure source bucket: %v", err)
	}
	const content = "original content, byte-identical on both sides"

	t.Run("matching_content_deletes_source", func(t *testing.T) {
		const key = "already-synced-match.txt"
		if _, err := src.PutObject(ctx, bucket, key, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("seed source: %v", err)
		}
		if err := dst.EnsureBucket(ctx, dstBucket); err != nil {
			t.Fatalf("ensure destination bucket: %v", err)
		}
		if _, err := dst.PutObject(ctx, dstBucket, key, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("seed destination: %v", err)
		}
		store := s.store(t)
		markAlreadySynced(t, ctx, store, src, bucket, key)

		syncer, err := tsync.New(tsync.Config{
			Source: src, Destination: dst, State: store,
			Buckets: map[string]tsync.BucketConfig{bucket: {Destination: dstBucket, BurnAfterReading: true}},
			Workers: 2,
		})
		if err != nil {
			t.Fatalf("create syncer: %v", err)
		}
		if err := syncer.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}

		if _, _, _, err := src.HeadObject(ctx, bucket, key); err == nil {
			t.Error("source object still exists after a verified matching sync")
		}
	})

	t.Run("tampered_destination_same_size_blocks_delete", func(t *testing.T) {
		const key = "already-synced-tampered.txt"
		if _, err := src.PutObject(ctx, bucket, key, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("seed source: %v", err)
		}
		const dstBucket2 = "bar-dst-tampered"
		if err := dst.EnsureBucket(ctx, dstBucket2); err != nil {
			t.Fatalf("ensure destination bucket: %v", err)
		}
		// Same length as content, different bytes: a size check alone would pass.
		tampered := strings.Repeat("X", len(content))
		if _, err := dst.PutObject(ctx, dstBucket2, key, strings.NewReader(tampered), int64(len(tampered))); err != nil {
			t.Fatalf("seed tampered destination: %v", err)
		}
		store := s.store(t)
		markAlreadySynced(t, ctx, store, src, bucket, key)

		syncer, err := tsync.New(tsync.Config{
			Source: src, Destination: dst, State: store,
			Buckets: map[string]tsync.BucketConfig{bucket: {Destination: dstBucket2, BurnAfterReading: true}},
			Workers: 2,
		})
		if err != nil {
			t.Fatalf("create syncer: %v", err)
		}
		// The job fails (checksum mismatch), so Run reports it, but the source
		// must survive regardless of whether the caller checks the error.
		_ = syncer.Run(ctx)

		if _, _, _, err := src.HeadObject(ctx, bucket, key); err != nil {
			t.Fatalf("source object was deleted despite a content mismatch: %v", err)
		}
	})

	t.Run("multipart_object_falls_back_to_content_and_deletes", func(t *testing.T) {
		// Above the transfer manager's 16 MiB multipart threshold, so both
		// uploads produce a composite ETag (a "-partCount" suffix) that the
		// ETag fast path must recognize as incomparable and skip, falling
		// back to downloading and hashing content instead.
		const key = "already-synced-multipart.bin"
		large := strings.Repeat("m", 17*1024*1024)
		if _, err := src.PutObject(ctx, bucket, key, strings.NewReader(large), int64(len(large))); err != nil {
			t.Fatalf("seed multipart source: %v", err)
		}
		const dstBucket3 = "bar-dst-multipart"
		if err := dst.EnsureBucket(ctx, dstBucket3); err != nil {
			t.Fatalf("ensure destination bucket: %v", err)
		}
		if _, err := dst.PutObject(ctx, dstBucket3, key, strings.NewReader(large), int64(len(large))); err != nil {
			t.Fatalf("seed multipart destination: %v", err)
		}

		_, _, dstETag, err := dst.HeadObject(ctx, dstBucket3, key)
		if err != nil {
			t.Fatalf("head destination: %v", err)
		}
		if !strings.Contains(dstETag, "-") {
			t.Fatalf("expected a composite (multipart) ETag, got %q — the 17 MiB body did not trigger multipart upload as this test assumes", dstETag)
		}

		store := s.store(t)
		markAlreadySynced(t, ctx, store, src, bucket, key)

		syncer, err := tsync.New(tsync.Config{
			Source: src, Destination: dst, State: store,
			Buckets: map[string]tsync.BucketConfig{bucket: {Destination: dstBucket3, BurnAfterReading: true}},
			Workers: 2,
		})
		if err != nil {
			t.Fatalf("create syncer: %v", err)
		}
		if err := syncer.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}

		if _, _, _, err := src.HeadObject(ctx, bucket, key); err == nil {
			t.Error("multipart source object still exists after a verified matching sync")
		}
	})
}
