package sync

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
)

func TestWorkerPoolRecoversJobPanic(t *testing.T) {
	counter, _ := noop.NewMeterProvider().Meter("t").Int64UpDownCounter("active")
	fn := func(_ context.Context, job Job) error {
		if job.Key == "bad" {
			panic("boom")
		}
		return errors.New("ok-err")
	}
	pool := newWorkerPool(context.Background(), 1, fn, counter)
	pool.submit(Job{SrcBucket: "b", Key: "bad"})
	pool.submit(Job{SrcBucket: "b", Key: "good"})
	pool.close()

	got := map[string]error{}
	for r := range pool.resultsCh() {
		got[r.Job.Key] = r.Err
	}
	if err := got["bad"]; err == nil || !strings.Contains(err.Error(), "panic in transfer: boom") {
		t.Errorf("bad job err = %v, want recovered panic", err)
	}
	if err := got["good"]; err == nil || err.Error() != "ok-err" {
		t.Errorf("good job err = %v, want ok-err: the worker must survive the panic", err)
	}
}

// A nil Destination client makes discovery dereference nil — a real panic on
// the per-bucket goroutine, which Run must report as that bucket's error.
func TestRunRecoversDiscoveryPanic(t *testing.T) {
	s, err := New(Config{Workers: 1, Buckets: map[string]BucketConfig{"b1": {Destination: "d1"}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "bucket b1: panic in discovery") {
		t.Fatalf("Run err = %v, want recovered discovery panic", err)
	}
}
