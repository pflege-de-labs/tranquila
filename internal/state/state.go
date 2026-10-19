package state

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StatusPending = "pending"
	StatusSynced  = "synced"
	StatusFailed  = "failed"

	scanBatchSize = 100

	// statsBuiltKey marks that per-bucket counters have been seeded. Deliberately
	// outside the tranquila:stats: prefix so it is never mistaken for a bucket.
	statsBuiltKey = "tranquila:statsbuilt"

	// bucketsKey indexes the buckets that have completed a discovery run.
	bucketsKey = "tranquila:buckets"

	// defaultCheckpointTTL bounds how long a sharded-discovery resume point
	// stays valid. A prefix whose walk was interrupted and never resumed — the
	// process down for a week, the bucket dropped from the mapping, the token
	// invalidated by a backend upgrade — must not pin discovery to a stale
	// continuation token forever. Once it expires the prefix is listed from the
	// start again, which is always correct, merely slower. Refreshed on every
	// save, so an actively advancing prefix never expires mid-walk.
	defaultCheckpointTTL = 24 * time.Hour
)

type ObjectState struct {
	Status     string
	ModifiedAt time.Time
	SyncedAt   time.Time
}

type BucketStats struct {
	Total   int64
	Synced  int64
	Pending int64
	Failed  int64
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	// PoolSize overrides go-redis's default (10 * GOMAXPROCS). 0 = library default.
	// Sized explicitly rather than left to GOMAXPROCS so it doesn't silently shrink
	// on a CPU-constrained pod, and so it can be reasoned about independent of the
	// container's CPU limit — see the pool-recovery note on dialErrorsNum below.
	PoolSize int
	// CheckpointTTL overrides how long a sharded-discovery resume point survives
	// without being refreshed. 0 = defaultCheckpointTTL.
	CheckpointTTL time.Duration
}

type Store struct {
	client        *redis.Client
	checkpointTTL time.Duration
}

func NewStore(cfg RedisConfig) (*Store, error) {
	// PoolSize also governs the pool's dial-error trip threshold: go-redis's
	// internal pool refuses to attempt a real dial once PoolSize consecutive
	// dial failures have accumulated, deferring to a single background prober
	// (once/sec) until it succeeds — see redis/go-redis#3062. A smaller, explicit
	// PoolSize reaches that (still self-healing) state sooner during an outage,
	// trading per-call redial log noise for a single steady prober.
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", cfg.Addr, err)
	}
	checkpointTTL := cfg.CheckpointTTL
	if checkpointTTL <= 0 {
		checkpointTTL = defaultCheckpointTTL
	}
	return &Store{client: client, checkpointTTL: checkpointTTL}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

// Ping verifies the Redis connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func objKey(bucket, key string) string {
	return "tranquila:obj:" + bucket + ":" + key
}

func collKey(bucket string) string {
	return "tranquila:collection:" + bucket
}

// ckptKey holds the resume point for one in-flight sharded-discovery prefix.
//
// Its own tranquila:ckpt: namespace, disjoint from every pattern this package
// SCANs: tranquila:obj:{bucket}:* (ScanPending, ScanStaleObjects,
// ScanAgedSyncedObjects, RebuildStats), tranquila:collection:*
// (listBucketsByScan) and tranquila:stats:* (staleStatsKeys). The counter Lua
// scripts only ever touch keys handed to them explicitly in KEYS, so they are
// unaffected too.
//
// Bucket names cannot contain ":" but prefixes can, which makes this key
// ambiguous to parse. That is deliberate and harmless: every access is by exact
// key and nothing enumerates checkpoints.
func ckptKey(bucket, prefix string) string {
	return "tranquila:ckpt:" + bucket + ":" + prefix
}

func statsKey(bucket string) string {
	return "tranquila:stats:" + bucket
}

// setStatusScript writes an object's status (plus any extra field/value pairs)
// and adjusts the bucket's counters in the same atomic step. Counting objects by
// scanning them is O(keyspace) per bucket, which does not scale; these counters
// make BucketStats a single HGETALL.
//
// KEYS: object key, stats key. ARGV: new status, then field/value pairs.
var setStatusScript = redis.NewScript(`
local old = redis.call('HGET', KEYS[1], 'status')
redis.call('HSET', KEYS[1], 'status', ARGV[1])
for i = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1])
end
if old == false then
  redis.call('HINCRBY', KEYS[2], 'total', 1)
  redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
elseif old ~= ARGV[1] then
  redis.call('HINCRBY', KEYS[2], old, -1)
  redis.call('HINCRBY', KEYS[2], ARGV[1], 1)
end
return 1
`)

// deleteObjectScript removes an object and decrements its bucket's counters.
// KEYS: object key, stats key.
var deleteObjectScript = redis.NewScript(`
local old = redis.call('HGET', KEYS[1], 'status')
local removed = redis.call('DEL', KEYS[1])
if removed == 1 then
  redis.call('HINCRBY', KEYS[2], 'total', -1)
  if old then
    redis.call('HINCRBY', KEYS[2], old, -1)
  end
end
return removed
`)

// setStatus applies a status transition and its counter update atomically.
func (s *Store) setStatus(ctx context.Context, bucket, key, status string, extra ...string) error {
	argv := make([]any, 0, len(extra)+1)
	argv = append(argv, status)
	for _, v := range extra {
		argv = append(argv, v)
	}
	return setStatusScript.Run(ctx, s.client,
		[]string{objKey(bucket, key), statsKey(bucket)}, argv...).Err()
}

func (s *Store) UpsertObject(ctx context.Context, bucket, key string, obj ObjectState) error {
	extra := []string{"modified_at", obj.ModifiedAt.UTC().Format(time.RFC3339Nano)}
	if !obj.SyncedAt.IsZero() {
		extra = append(extra, "synced_at", obj.SyncedAt.UTC().Format(time.RFC3339Nano))
	}
	return s.setStatus(ctx, bucket, key, obj.Status, extra...)
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (*ObjectState, error) {
	vals, err := s.client.HGetAll(ctx, objKey(bucket, key)).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall %s/%s: %w", bucket, key, err)
	}
	if len(vals) == 0 {
		return nil, nil
	}
	obj := &ObjectState{Status: vals["status"]}
	if v, ok := vals["modified_at"]; ok {
		obj.ModifiedAt, _ = time.Parse(time.RFC3339Nano, v)
	}
	if v, ok := vals["synced_at"]; ok {
		obj.SyncedAt, _ = time.Parse(time.RFC3339Nano, v)
	}
	return obj, nil
}

func (s *Store) MarkSynced(ctx context.Context, bucket, key string) error {
	return s.setStatus(ctx, bucket, key, StatusSynced,
		"synced_at", time.Now().UTC().Format(time.RFC3339Nano))
}

func (s *Store) MarkFailed(ctx context.Context, bucket, key string) error {
	return s.setStatus(ctx, bucket, key, StatusFailed)
}

// RemoveObject deletes the tracking record for an object that has been fully
// processed in burn-after-reading mode (source object deleted from source bucket).
func (s *Store) RemoveObject(ctx context.Context, bucket, key string) error {
	return deleteObjectScript.Run(ctx, s.client,
		[]string{objKey(bucket, key), statsKey(bucket)}).Err()
}

func (s *Store) MarkPending(ctx context.Context, bucket, key string, modifiedAt time.Time) error {
	return s.setStatus(ctx, bucket, key, StatusPending,
		"modified_at", modifiedAt.UTC().Format(time.RFC3339Nano))
}

// ScanPending returns all object keys with status=pending for a bucket.
func (s *Store) ScanPending(ctx context.Context, bucket string) ([]string, error) {
	pattern := "tranquila:obj:" + bucket + ":*"
	prefix := "tranquila:obj:" + bucket + ":"

	var keys []string
	iter := s.client.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan keys for bucket %s: %w", bucket, err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	var pending []string
	for i := 0; i < len(keys); i += scanBatchSize {
		end := min(i+scanBatchSize, len(keys))
		batch := keys[i:end]

		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for j, k := range batch {
			cmds[j] = pipe.HGet(ctx, k, "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("pipeline hget: %w", err)
		}
		for j, cmd := range cmds {
			if cmd.Val() == StatusPending {
				pending = append(pending, strings.TrimPrefix(batch[j], prefix))
			}
		}
	}
	return pending, nil
}

// TouchSeen records that an object was observed in the source bucket during a
// discovery pass, without altering its status or bucket counters. Used by
// propagate-deletes reconciliation to tell "still present in source" apart
// from "no longer seen" for objects that needsSync otherwise never touches
// again once synced.
func (s *Store) TouchSeen(ctx context.Context, bucket, key string, seenAt time.Time) error {
	return s.client.HSet(ctx, objKey(bucket, key), "seen_at", seenAt.UTC().Format(time.RFC3339Nano)).Err()
}

// ScanStaleObjects returns keys for a bucket whose status is "synced" and
// whose seen_at is missing or predates `before` — i.e. objects propagate-deletes
// reconciliation has not observed in the most recent source listing and so
// suspects were deleted from source. Callers must verify against the source
// before deleting the destination object; this only reports candidates.
//
// Like ScanPending, this scans tranquila:obj:{bucket}:* — SCAN filters MATCH
// server-side after walking the whole keyspace, so this call costs O(total
// keyspace) regardless of the bucket's size (see RebuildStats and this
// project's Redis Key Design notes). Only call it for buckets that opted into
// propagate-deletes, and no more often than the configured reconcile interval.
func (s *Store) ScanStaleObjects(ctx context.Context, bucket string, before time.Time) ([]string, error) {
	pattern := "tranquila:obj:" + bucket + ":*"
	prefix := "tranquila:obj:" + bucket + ":"

	var keys []string
	iter := s.client.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan keys for bucket %s: %w", bucket, err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	var stale []string
	for i := 0; i < len(keys); i += scanBatchSize {
		end := min(i+scanBatchSize, len(keys))
		batch := keys[i:end]

		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, len(batch))
		for j, k := range batch {
			cmds[j] = pipe.HMGet(ctx, k, "status", "seen_at")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("pipeline hmget: %w", err)
		}
		for j, cmd := range cmds {
			vals := cmd.Val()
			// Short only if the command failed; guard so tolerating per-command errors later cannot panic.
			if len(vals) < 2 {
				continue
			}
			status, _ := vals[0].(string)
			if status != StatusSynced {
				continue
			}
			seenAtStr, _ := vals[1].(string)
			if seenAtStr == "" {
				stale = append(stale, strings.TrimPrefix(batch[j], prefix))
				continue
			}
			seenAt, err := time.Parse(time.RFC3339Nano, seenAtStr)
			if err != nil || seenAt.Before(before) {
				stale = append(stale, strings.TrimPrefix(batch[j], prefix))
			}
		}
	}
	return stale, nil
}

// ScanAgedSyncedObjects returns keys for a bucket whose status is "synced" and
// whose modified_at is at or before `before` — candidates for burn-after-
// reading's minimum-age reconciliation. Reuses the modified_at field every
// object record already carries (written by MarkPending/UpsertObject); unlike
// ScanStaleObjects, no extra per-object write is needed for this to work. A
// record with a missing or unparseable modified_at is excluded rather than
// treated as a candidate — burn-after-reading must never delete a source
// object whose age it cannot actually confirm.
//
// Like ScanStaleObjects, this scans tranquila:obj:{bucket}:* — SCAN filters
// MATCH server-side after walking the whole keyspace, so this call costs
// O(total keyspace) regardless of the bucket's size (see RebuildStats and
// this project's Redis Key Design notes). Only call it for buckets with a
// configured burn-after-reading minimum age, on the reconciler's own
// (deliberately infrequent) schedule.
func (s *Store) ScanAgedSyncedObjects(ctx context.Context, bucket string, before time.Time) ([]string, error) {
	pattern := "tranquila:obj:" + bucket + ":*"
	prefix := "tranquila:obj:" + bucket + ":"

	var keys []string
	iter := s.client.Scan(ctx, 0, pattern, scanBatchSize).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan keys for bucket %s: %w", bucket, err)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	var aged []string
	for i := 0; i < len(keys); i += scanBatchSize {
		end := min(i+scanBatchSize, len(keys))
		batch := keys[i:end]

		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, len(batch))
		for j, k := range batch {
			cmds[j] = pipe.HMGet(ctx, k, "status", "modified_at")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("pipeline hmget: %w", err)
		}
		for j, cmd := range cmds {
			vals := cmd.Val()
			// Short only if the command failed; guard so tolerating per-command errors later cannot panic.
			if len(vals) < 2 {
				continue
			}
			status, _ := vals[0].(string)
			if status != StatusSynced {
				continue
			}
			modifiedAtStr, _ := vals[1].(string)
			modifiedAt, err := time.Parse(time.RFC3339Nano, modifiedAtStr)
			if err != nil {
				continue
			}
			if !modifiedAt.After(before) {
				aged = append(aged, strings.TrimPrefix(batch[j], prefix))
			}
		}
	}
	return aged, nil
}

func (s *Store) SetCollectionTime(ctx context.Context, bucket string, t time.Time) error {
	pipe := s.client.Pipeline()
	pipe.Set(ctx, collKey(bucket), t.UTC().Format(time.RFC3339Nano), 0)
	// Maintain the bucket index alongside, so ListBuckets need not scan.
	pipe.SAdd(ctx, bucketsKey, bucket)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *Store) GetCollectionTime(ctx context.Context, bucket string) (time.Time, error) {
	v, err := s.client.Get(ctx, collKey(bucket)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("get collection time for %s: %w", bucket, err)
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse collection time: %w", err)
	}
	return t, nil
}

// ListBuckets returns the names of all source buckets that have completed at
// least one discovery run (i.e. have a stored collection timestamp).
//
// Served from a maintained index: scanning for the collection keys would
// traverse the entire keyspace, which is dominated by object records.
func (s *Store) ListBuckets(ctx context.Context) ([]string, error) {
	indexed, err := s.client.Exists(ctx, bucketsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("check bucket index: %w", err)
	}
	if indexed == 1 {
		buckets, err := s.client.SMembers(ctx, bucketsKey).Result()
		if err != nil {
			return nil, fmt.Errorf("read bucket index: %w", err)
		}
		return buckets, nil
	}
	// No index yet (a keyspace written before it existed): fall back to the scan
	// and seed it, so this happens at most once.
	buckets, err := s.listBucketsByScan(ctx)
	if err != nil || len(buckets) == 0 {
		return buckets, err
	}
	members := make([]any, len(buckets))
	for i, b := range buckets {
		members[i] = b
	}
	if err := s.client.SAdd(ctx, bucketsKey, members...).Err(); err != nil {
		return nil, fmt.Errorf("seed bucket index: %w", err)
	}
	return buckets, nil
}

// listBucketsByScan enumerates buckets from their collection keys. O(keyspace);
// used only to seed or reconcile the index.
func (s *Store) listBucketsByScan(ctx context.Context) ([]string, error) {
	const prefix = "tranquila:collection:"
	var buckets []string
	iter := s.client.Scan(ctx, 0, prefix+"*", scanBatchSize).Iterator()
	for iter.Next(ctx) {
		buckets = append(buckets, strings.TrimPrefix(iter.Val(), prefix))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan collection keys: %w", err)
	}
	return buckets, nil
}

// BucketStats reads a bucket's maintained counters. It is O(1): a single
// HGETALL, rather than a scan of the whole keyspace.
func (s *Store) BucketStats(ctx context.Context, bucket string) (BucketStats, error) {
	if err := s.ensureStatsBuilt(ctx); err != nil {
		return BucketStats{}, err
	}
	vals, err := s.client.HGetAll(ctx, statsKey(bucket)).Result()
	if err != nil {
		return BucketStats{}, fmt.Errorf("hgetall stats for %s: %w", bucket, err)
	}
	n := func(field string) int64 {
		v, _ := strconv.ParseInt(vals[field], 10, 64)
		return v
	}
	return BucketStats{
		Total:   n("total"),
		Synced:  n(StatusSynced),
		Pending: n(StatusPending),
		Failed:  n(StatusFailed),
	}, nil
}

// ensureStatsBuilt rebuilds the counters once, for keyspaces written before they
// existed. The marker is checked per call rather than cached in memory so that a
// failed rebuild is retried and every replica converges.
func (s *Store) ensureStatsBuilt(ctx context.Context) error {
	built, err := s.client.Exists(ctx, statsBuiltKey).Result()
	if err != nil {
		return fmt.Errorf("check stats marker: %w", err)
	}
	if built == 1 {
		return nil
	}
	return s.RebuildStats(ctx)
}

// RebuildStats recomputes every bucket's counters from the object records in a
// single pass over the keyspace, and marks them built.
//
// This is the expensive path — O(keyspace) — and exists to seed counters for
// existing data and to reconcile drift. It deliberately scans once for all
// buckets: SCAN with MATCH filters server-side after iterating, so a per-bucket
// pattern would traverse the whole keyspace once per bucket.
func (s *Store) RebuildStats(ctx context.Context) error {
	const prefix = "tranquila:obj:"
	counts := make(map[string]*BucketStats)

	var batch []string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		pipe := s.client.Pipeline()
		cmds := make([]*redis.StringCmd, len(batch))
		for i, k := range batch {
			cmds[i] = pipe.HGet(ctx, k, "status")
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("pipeline hget: %w", err)
		}
		for i, cmd := range cmds {
			// Bucket names cannot contain ":", but object keys can, so split on
			// the first separator after the prefix.
			bucket, _, ok := strings.Cut(strings.TrimPrefix(batch[i], prefix), ":")
			if !ok {
				continue
			}
			st, seen := counts[bucket]
			if !seen {
				st = &BucketStats{}
				counts[bucket] = st
			}
			st.Total++
			switch cmd.Val() {
			case StatusSynced:
				st.Synced++
			case StatusPending:
				st.Pending++
			case StatusFailed:
				st.Failed++
			}
		}
		batch = batch[:0]
		return nil
	}

	iter := s.client.Scan(ctx, 0, prefix+"*", scanBatchSize).Iterator()
	for iter.Next(ctx) {
		batch = append(batch, iter.Val())
		if len(batch) >= scanBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan object keys: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}

	// Include buckets that have been discovered but hold no objects, so their
	// counters read as zero rather than as never-built. Read from the collection
	// keys rather than the bucket index, since a reconcile must not trust the
	// index it is also rebuilding.
	buckets, err := s.listBucketsByScan(ctx)
	if err != nil {
		return err
	}
	for _, b := range buckets {
		if _, ok := counts[b]; !ok {
			counts[b] = &BucketStats{}
		}
	}

	// Drop counters for buckets that no longer have objects or a collection
	// record, so a reconcile clears stale entries instead of leaving them.
	stale, err := s.staleStatsKeys(ctx, counts)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	for _, k := range stale {
		pipe.Del(ctx, k)
	}
	// Reconcile the bucket index from the collection keys in the same pass.
	pipe.Del(ctx, bucketsKey)
	if len(buckets) > 0 {
		members := make([]any, len(buckets))
		for i, b := range buckets {
			members[i] = b
		}
		pipe.SAdd(ctx, bucketsKey, members...)
	}
	for bucket, st := range counts {
		pipe.HSet(ctx, statsKey(bucket), map[string]any{
			"total":       st.Total,
			StatusSynced:  st.Synced,
			StatusPending: st.Pending,
			StatusFailed:  st.Failed,
		})
	}
	pipe.Set(ctx, statsBuiltKey, time.Now().UTC().Format(time.RFC3339Nano), 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("write stats: %w", err)
	}
	return nil
}

// staleStatsKeys returns counter keys whose bucket is absent from counts.
func (s *Store) staleStatsKeys(ctx context.Context, counts map[string]*BucketStats) ([]string, error) {
	const prefix = "tranquila:stats:"
	var stale []string
	iter := s.client.Scan(ctx, 0, prefix+"*", scanBatchSize).Iterator()
	for iter.Next(ctx) {
		if _, ok := counts[strings.TrimPrefix(iter.Val(), prefix)]; !ok {
			stale = append(stale, iter.Val())
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan stats keys: %w", err)
	}
	return stale, nil
}

// LoadCheckpoint returns the stored continuation token for (bucket, prefix), or
// "" when there is none — never checkpointed, already completed, or expired.
// Implements storage.DiscoveryCheckpointer.
func (s *Store) LoadCheckpoint(ctx context.Context, bucket, prefix string) (string, error) {
	tok, err := s.client.HGet(ctx, ckptKey(bucket, prefix), "token").Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load discovery checkpoint %s/%s: %w", bucket, prefix, err)
	}
	return tok, nil
}

// SaveCheckpoint records token as the resume point for (bucket, prefix) and
// refreshes its TTL, so a prefix that is actively advancing stays checkpointed
// while an abandoned one expires on its own.
func (s *Store) SaveCheckpoint(ctx context.Context, bucket, prefix, token string) error {
	if token == "" {
		return s.ClearCheckpoint(ctx, bucket, prefix)
	}
	key := ckptKey(bucket, prefix)
	pipe := s.client.Pipeline()
	pipe.HSet(ctx, key, "token", token, "updated_at", time.Now().UTC().Format(time.RFC3339Nano))
	pipe.Expire(ctx, key, s.checkpointTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("save discovery checkpoint %s/%s: %w", bucket, prefix, err)
	}
	return nil
}

// ClearCheckpoint drops a prefix's resume point so the next cycle lists it in
// full. Called when a prefix completes — that full re-list is what gives an
// object whose transfer failed a chance to be rediscovered.
func (s *Store) ClearCheckpoint(ctx context.Context, bucket, prefix string) error {
	if err := s.client.Del(ctx, ckptKey(bucket, prefix)).Err(); err != nil {
		return fmt.Errorf("clear discovery checkpoint %s/%s: %w", bucket, prefix, err)
	}
	return nil
}
