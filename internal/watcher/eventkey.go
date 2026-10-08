package watcher

import (
	"net/url"

	"github.com/rs/zerolog/log"
)

// decodeEventKey undoes the form encoding AWS applies to the object key in
// S3->SQS event notifications (space as "+", everything else percent-escaped).
// Not used for MinIO: ListenBucketNotification delivers keys unencoded. A key
// that fails to decode is passed through raw: dropping the event would be
// worse than a sync attempt that may 404, and sync is idempotent.
func decodeEventKey(bucket, raw string) string {
	key, err := url.QueryUnescape(raw)
	if err != nil {
		log.Warn().Err(err).Str("bucket", bucket).Str("key", raw).
			Msg("event key is not URL-encoded as expected, using it raw")
		return raw
	}
	return key
}
