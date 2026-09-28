package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/notification"
)

// makeNotifyInfo builds a notification.Info by JSON round-trip, avoiding the
// unexported eventMeta type from minio-go.
func makeNotifyInfo(bucket, key string, size int64) notification.Info {
	return makeNotifyInfoWithEvent(bucket, key, size, "s3:ObjectCreated:Put")
}

func makeNotifyInfoWithEvent(bucket, key string, size int64, eventName string) notification.Info {
	type obj struct {
		Key  string `json:"key"`
		Size int64  `json:"size,omitempty"`
	}
	type bkt struct {
		Name string `json:"name"`
	}
	type s3meta struct {
		Bucket bkt `json:"bucket"`
		Object obj `json:"object"`
	}
	type eventJSON struct {
		EventName string `json:"eventName"`
		S3        s3meta `json:"s3"`
	}
	type infoJSON struct {
		Records []eventJSON `json:"Records"`
	}
	raw, _ := json.Marshal(infoJSON{Records: []eventJSON{{
		EventName: eventName,
		S3:        s3meta{Bucket: bkt{Name: bucket}, Object: obj{Key: key, Size: size}},
	}}})
	var info notification.Info
	_ = json.Unmarshal(raw, &info)
	return info
}

// fakeMinIONotifier implements minioNotifier for testing.
type fakeMinIONotifier struct {
	infos []notification.Info
	block bool // if true, blocks until ctx is cancelled before closing
}

func (f *fakeMinIONotifier) ListenBucketNotification(ctx context.Context, _, _, _ string, _ []string) <-chan notification.Info {
	ch := make(chan notification.Info, len(f.infos))
	for _, info := range f.infos {
		ch <- info
	}
	if f.block {
		go func() {
			<-ctx.Done()
			close(ch)
		}()
	} else {
		close(ch)
	}
	return ch
}

func TestNewMinIO(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MinIOConfig
		wantErr bool
	}{
		{
			name:    "plain_host_port",
			cfg:     MinIOConfig{Endpoint: "localhost:9000", AccessKey: "key", SecretKey: "secret"},
			wantErr: false,
		},
		{
			name:    "full_http_url",
			cfg:     MinIOConfig{Endpoint: "http://localhost:9000", AccessKey: "key", SecretKey: "secret"},
			wantErr: false,
		},
		{
			name:    "https_url_sets_secure",
			cfg:     MinIOConfig{Endpoint: "https://play.min.io", AccessKey: "key", SecretKey: "secret", Secure: true},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, err := NewMinIO(tc.cfg)
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tc.wantErr && w == nil {
				t.Error("expected non-nil watcher")
			}
		})
	}
}

func TestMinIOWatchBucket(t *testing.T) {
	tests := []struct {
		name       string
		infos      []notification.Info
		wantEvents []ObjectEvent
	}{
		{
			name:       "single_event_emitted",
			infos:      []notification.Info{makeNotifyInfo("b1", "k1", 100)},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "k1", Size: 100}},
		},
		{
			name: "error_info_skipped",
			infos: []notification.Info{
				{Err: errors.New("stream error")},
				makeNotifyInfo("b1", "k2", 200),
			},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "k2", Size: 200}},
		},
		{
			name:       "no_events",
			infos:      nil,
			wantEvents: nil,
		},
		{
			name:       "delete_event_sets_isdelete",
			infos:      []notification.Info{makeNotifyInfoWithEvent("b1", "k3", 0, "s3:ObjectRemoved:Delete")},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "k3", IsDelete: true}},
		},
		{
			name:       "delete_marker_event_sets_isdelete",
			infos:      []notification.Info{makeNotifyInfoWithEvent("b1", "k4", 0, "s3:ObjectRemoved:DeleteMarkerCreated")},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "k4", IsDelete: true}},
		},
		{
			// ListenBucketNotification delivers keys unencoded (verified against
			// a real server), unlike S3->SQS; decoding here would turn "+" into " ".
			name:       "raw_key_passed_through_undecoded",
			infos:      []notification.Info{makeNotifyInfo("b1", "dir/a b+c%2B.txt", 7)},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "dir/a b+c%2B.txt", Size: 7}},
		},
		{
			name:       "created_event_leaves_isdelete_false",
			infos:      []notification.Info{makeNotifyInfoWithEvent("b1", "k5", 5, "s3:ObjectCreated:Put")},
			wantEvents: []ObjectEvent{{Bucket: "b1", Key: "k5", Size: 5, IsDelete: false}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &MinIOWatcher{client: &fakeMinIONotifier{infos: tc.infos}}
			out := make(chan ObjectEvent, 100)
			w.watchBucket(context.Background(), "b1", out)
			close(out)

			var got []ObjectEvent
			for e := range out {
				got = append(got, e)
			}

			if len(got) != len(tc.wantEvents) {
				t.Fatalf("got %d events, want %d", len(got), len(tc.wantEvents))
			}
			for i, e := range got {
				if e != tc.wantEvents[i] {
					t.Errorf("event[%d] = %+v, want %+v", i, e, tc.wantEvents[i])
				}
			}
		})
	}
}

func TestMinIOWatchMultipleBuckets(t *testing.T) {
	buckets := []string{"b1", "b2"}
	w := &MinIOWatcher{client: &fakeMinIONotifier{
		infos: []notification.Info{makeNotifyInfo("bucket", "k", 10)},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ch, err := w.Watch(ctx, buckets)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	var got []ObjectEvent
	for e := range ch {
		got = append(got, e)
	}
	// Two buckets, one event each → 2 events total.
	if len(got) != 2 {
		t.Errorf("got %d events, want 2", len(got))
	}
}

func TestMinIOWatchContextCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	w := &MinIOWatcher{client: &fakeMinIONotifier{block: true}}

	ch, err := w.Watch(ctx, []string{"b1", "b2"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel, got event")
		}
	case <-time.After(time.Second):
		t.Error("channel did not close after context cancel")
	}
}
