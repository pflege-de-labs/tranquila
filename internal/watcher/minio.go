package watcher

import (
	"context"
	"net/url"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/rs/zerolog/log"
)

// MinIOConfig holds connection details for the MinIO native event stream.
type MinIOConfig struct {
	Endpoint  string // host:port or full URL
	AccessKey string
	SecretKey string
	Secure    bool // true for HTTPS
}

// minioNotifier is the subset of minio.Client used by MinIOWatcher, allowing
// injection of fakes in tests.
type minioNotifier interface {
	ListenBucketNotification(ctx context.Context, bucketName, prefix, suffix string, events []string) <-chan notification.Info
}

// MinIOWatcher subscribes to MinIO's ListenBucketNotification SSE stream.
type MinIOWatcher struct {
	client minioNotifier
	sleep  sleeper // nil = waitOrDone
}

// NewMinIO creates a MinIOWatcher using ListenBucketNotification for real-time events.
func NewMinIO(cfg MinIOConfig) (*MinIOWatcher, error) {
	endpoint := cfg.Endpoint
	// Strip scheme if present — minio-go expects host:port only.
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		endpoint = u.Host
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.Secure,
	})
	if err != nil {
		return nil, err
	}
	return &MinIOWatcher{client: client}, nil
}

// Watch subscribes to object-created and object-removed events for each bucket
// and fans all notifications into a single channel. The channel is closed when
// ctx is cancelled or all per-bucket streams end.
func (w *MinIOWatcher) Watch(ctx context.Context, buckets []string) (<-chan ObjectEvent, error) {
	out := make(chan ObjectEvent, 100)

	var wg sync.WaitGroup
	for _, b := range buckets {
		wg.Add(1)
		go func(bucket string) {
			defer wg.Done()
			w.watchBucket(ctx, bucket, out)
		}(b)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out, nil
}

func (w *MinIOWatcher) watchBucket(ctx context.Context, bucket string, out chan<- ObjectEvent) {
	events := []string{
		string(notification.ObjectCreatedAll),
		string(notification.ObjectRemovedAll),
	}
	sleep := w.sleep
	if sleep == nil {
		sleep = waitOrDone
	}
	// minio-go closes the stream after a failed request or non-200 instead of
	// retrying, so resubscribe or this bucket silently stops receiving events.
	failures := 0
	for {
		if w.drain(bucket, w.client.ListenBucketNotification(ctx, bucket, "", "", events), out) {
			failures = 0
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		delay := retryDelay(failures)
		log.Warn().Str("bucket", bucket).Int("attempt", failures).Str("retry_in", delay.String()).
			Msg("minio notification stream ended, resubscribing")
		if !sleep(ctx, delay) {
			return
		}
	}
}

// drain forwards one subscription's events until its stream closes, reporting
// whether it delivered any records (i.e. the subscription was healthy).
func (w *MinIOWatcher) drain(bucket string, ch <-chan notification.Info, out chan<- ObjectEvent) bool {
	healthy := false
	for info := range ch {
		if info.Err != nil {
			log.Warn().Err(info.Err).Str("bucket", bucket).Msg("minio notification error")
			continue
		}
		for _, rec := range info.Records {
			healthy = true
			out <- ObjectEvent{
				Bucket:   rec.S3.Bucket.Name,
				Key:      rec.S3.Object.Key,
				Size:     rec.S3.Object.Size,
				IsDelete: strings.HasPrefix(rec.EventName, "s3:ObjectRemoved"),
			}
		}
	}
	return healthy
}
