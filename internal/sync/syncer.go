package sync

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pflege-de-labs/tranquila/internal/recovery"
	"github.com/pflege-de-labs/tranquila/internal/state"
	"github.com/pflege-de-labs/tranquila/internal/storage"
	"github.com/pflege-de-labs/tranquila/internal/watcher"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const defaultDiscoveryBatchSize = 100_000

// BucketConfig holds destination routing and path-prefix configuration for a source bucket.
type BucketConfig struct {
	Destination      string // destination bucket name
	SrcPrefix        string // list/filter prefix applied when scanning the source; empty = all objects
	DstPrefix        string // replaces SrcPrefix in the destination key; empty = keep original key
	BurnAfterReading bool   // delete source object after verified sync
	// BurnAfterReadingMinAge defers a burn until the source object is at least
	// this old (by its S3 last-modified time). 0 = delete immediately after
	// verified sync, matching burn-after-reading's original behavior. Already
	// resolved (global default merged with any per-bucket override) by the
	// time it reaches this struct — see config.Duration/cmd_sync.go.
	BurnAfterReadingMinAge time.Duration
	PropagateDeletes       bool // delete destination object when the source object is deleted
	// ShardedDiscovery skips the flat (bucket-wide) listing entirely and always
	// discovers via a recursive, "/"-delimited tree walk instead. Set this for a
	// bucket already known to be too large/slow for a flat listing to complete —
	// otherwise this triggers automatically the first time a flat listing
	// exhausts its retries with a transient/throttle error (see
	// isShardableListErr), at the one-time cost of that exhausted retry budget.
	ShardedDiscovery bool
}

// destKey returns the destination object key for srcKey, applying prefix replacement when configured.
func (bc BucketConfig) destKey(srcKey string) string {
	if bc.SrcPrefix == "" || bc.DstPrefix == "" {
		return srcKey
	}
	return bc.DstPrefix + strings.TrimPrefix(srcKey, bc.SrcPrefix)
}

type Config struct {
	Source              *storage.Client
	Destination         *storage.Client
	State               *state.Store
	Meter               metric.Meter            // optional; zero value produces no-op instruments
	Buckets             map[string]BucketConfig // src → config; nil = auto-discover all
	DestBucketPrefix    string                  // prefix for auto-discovered destination bucket names
	Workers             int
	CheckSizes          bool      // re-queue synced objects whose destination size differs from source
	DryRun              bool      // log planned burn-after-reading deletions without executing them
	Progress            *Progress // optional; enables live progress tracking for the management API
	DiscoveryBatchSize  int       // max objects to discover per bucket before syncing (0 = default 100 000)
	MaxWorkersPerBucket int       // cap on concurrent transfers for a single bucket (0 = default: half of Workers)

	CycleBackoff    time.Duration // base delay before retrying a failed watch cycle (0 = 5s)
	CycleBackoffMax time.Duration // ceiling for the retry delay (0 = 10m)

	// DeleteReconcileInterval throttles how often propagate-deletes reconciliation
	// (an O(keyspace) Redis scan per bucket, see ScanStaleObjects) runs per bucket,
	// independent of the sync/discovery cadence. 0 = reconcile at the end of every
	// Run().
	DeleteReconcileInterval time.Duration
}

type metrics struct {
	synced           metric.Int64Counter
	failed           metric.Int64Counter
	bytesTransferred metric.Int64Counter
	duration         metric.Float64Histogram
	activeWorkers    metric.Int64UpDownCounter
	cycleFailures    metric.Int64Counter
}

type Syncer struct {
	cfg Config
	m   metrics

	reconcileMu   sync.Mutex
	lastReconcile map[string]time.Time // bucket -> last propagate-deletes reconcile
}

func New(cfg Config) (*Syncer, error) {
	m, err := newMetrics(cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("init metrics: %w", err)
	}
	return &Syncer{cfg: cfg, m: m, lastReconcile: make(map[string]time.Time)}, nil
}

func newMetrics(meter metric.Meter) (metrics, error) {
	// metric.Meter is an interface, so its zero value is nil rather than a no-op.
	if meter == nil {
		meter = noop.Meter{}
	}
	synced, err := meter.Int64Counter("tranquila.objects.synced",
		metric.WithDescription("Total objects successfully synced"))
	if err != nil {
		return metrics{}, err
	}
	failed, err := meter.Int64Counter("tranquila.objects.failed",
		metric.WithDescription("Total object sync failures"))
	if err != nil {
		return metrics{}, err
	}
	bytes, err := meter.Int64Counter("tranquila.bytes.transferred",
		metric.WithDescription("Total bytes transferred"),
		metric.WithUnit("By"))
	if err != nil {
		return metrics{}, err
	}
	dur, err := meter.Float64Histogram("tranquila.transfer.duration",
		metric.WithDescription("Transfer duration per object"),
		metric.WithUnit("s"))
	if err != nil {
		return metrics{}, err
	}
	activeWorkers, err := meter.Int64UpDownCounter("tranquila.workers.active",
		metric.WithDescription("Number of workers currently executing a transfer"))
	if err != nil {
		return metrics{}, err
	}
	cycleFailures, err := meter.Int64Counter("tranquila.sync.cycle.failures",
		metric.WithDescription("Watch cycles that failed and were retried with backoff"))
	if err != nil {
		return metrics{}, err
	}
	return metrics{
		synced:           synced,
		failed:           failed,
		bytesTransferred: bytes,
		duration:         dur,
		activeWorkers:    activeWorkers,
		cycleFailures:    cycleFailures,
	}, nil
}

// discoveryBatchSize returns the effective batch size: cfg value if positive, default otherwise.
func (s *Syncer) discoveryBatchSize() int {
	if s.cfg.DiscoveryBatchSize > 0 {
		return s.cfg.DiscoveryBatchSize
	}
	return defaultDiscoveryBatchSize
}

// maxWorkersPerBucket returns the effective per-bucket transfer concurrency cap:
// cfg value if positive, otherwise half of Workers (min 1). This keeps one
// bucket with a large or slow listing (e.g. many small objects) from occupying
// the whole shared worker pool and starving other buckets' transfers.
func (s *Syncer) maxWorkersPerBucket() int {
	if s.cfg.MaxWorkersPerBucket > 0 {
		return s.cfg.MaxWorkersPerBucket
	}
	if n := (s.cfg.Workers + 1) / 2; n > 0 {
		return n
	}
	return 1
}

// Run performs discovery and sync for all configured buckets. Each bucket's
// discovery runs concurrently with other buckets; sync starts as soon as a
// bucket's discovery batch is ready. Large buckets are processed in batches of
// DiscoveryBatchSize objects so that sync begins without waiting for the full
// listing and memory usage stays bounded.
func (s *Syncer) Run(ctx context.Context) error {
	if s.cfg.Progress != nil {
		s.cfg.Progress.start(time.Now().UTC())
		defer s.cfg.Progress.stop()
	}

	bucketMap, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	srcs := make([]string, 0, len(bucketMap))
	for src := range bucketMap {
		srcs = append(srcs, src)
	}
	log.Info().Strs("buckets", srcs).Msg("starting discovery")

	pool := newWorkerPool(ctx, s.cfg.Workers, s.transfer, s.m.activeWorkers)

	var resultWg sync.WaitGroup
	resultWg.Go(func() {
		s.processResults(ctx, pool.resultsCh())
	})

	collectionTime := time.Now().UTC()
	sem := make(chan struct{}, s.cfg.Workers)
	errc := make(chan error, len(bucketMap))
	var discoverWg sync.WaitGroup

	for bucket, bc := range bucketMap {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		discoverWg.Add(1)
		go func(b string, cfg BucketConfig) {
			defer discoverWg.Done()
			defer func() { <-sem }()
			// Covers onPage too: both discovery strategies call it on this goroutine.
			defer func() {
				if r := recover(); r != nil {
					errc <- fmt.Errorf("bucket %s: %w", b, recovery.Handle("discovery", r, log.With().Str("bucket", b).Logger()))
				}
			}()
			if err := s.discoverAndSyncBucket(ctx, b, cfg, collectionTime, pool); err != nil {
				if !errors.Is(err, context.Canceled) {
					errc <- fmt.Errorf("bucket %s: %w", b, err)
				}
			}
		}(bucket, bc)
	}

	discoverWg.Wait()
	pool.close()
	resultWg.Wait()
	close(errc)

	var errs []error
	for err := range errc {
		errs = append(errs, err)
	}

	if ctx.Err() == nil && s.cfg.State != nil {
		now := time.Now().UTC()
		for bucket, bc := range bucketMap {
			if !bc.PropagateDeletes || !s.reconcileDue(bucket, now) {
				continue
			}
			if err := s.reconcileDeletes(ctx, bucket, bc, collectionTime); err != nil && !errors.Is(err, context.Canceled) {
				errs = append(errs, fmt.Errorf("reconcile deletes %s: %w", bucket, err))
			}
		}
	}

	// Join rather than first-error: every bucket's failure stays visible, and a
	// mixed cycle is not misread as pure misconfiguration by isFatalCycleErr.
	return errors.Join(errs...)
}

// isShardableListErr reports whether err is a listing failure (as opposed to
// some other discovery-pipeline error, e.g. a Redis mark-pending failure —
// storage.ListError distinguishes these) classified transient/throttled: the
// class of failure prefix-sharded discovery might actually help with, since
// many narrower listing calls to the same backend could succeed where one
// flat, bucket-wide call couldn't. A permanent error (bad credentials, no
// such bucket) would fail identically either way, so it's not shardable.
func isShardableListErr(err error) bool {
	var listErr *storage.ListError
	if !errors.As(err, &listErr) {
		return false
	}
	// A listing too slow to finish within its per-attempt timeout is exactly
	// what sharding remedies, but storage.Classify deliberately calls
	// context.DeadlineExceeded ClassOK ("cancellation is our own doing"), so
	// this shape has to be recognised here. Only DeadlineExceeded — a plain
	// context.Canceled is SIGTERM, not a struggling backend.
	if errors.Is(listErr.Err, context.DeadlineExceeded) {
		return true
	}
	class := storage.Classify(listErr.Err)
	return class == storage.ClassTransient || class == storage.ClassThrottle
}

// discoverAndSyncBucket lists the source bucket, marks each object pending in
// Redis, and submits transfer jobs to the worker pool as objects are
// discovered. A per-bucket semaphore caps how many of this bucket's transfers
// may be in flight at once, so a bucket with many (small) objects cannot
// occupy the whole shared worker pool and starve other buckets. Called
// concurrently by Run for each bucket.
//
// Discovery uses a flat (bucket-wide) listing by default, batched via
// DiscoveryBatchSize (see discoverFlat). cfg.ShardedDiscovery skips straight
// to a recursive, "/"-delimited tree walk instead (see discoverSharded) for a
// bucket already known to be too large for a flat listing to complete. For an
// unflagged bucket, a flat listing that exhausts its retries with a
// transient/throttle error automatically falls back to the sharded walk for
// the rest of this cycle — at the one-time cost of the exhausted retry
// budget, which is why the flag exists to skip that cost on repeat cycles.
func (s *Syncer) discoverAndSyncBucket(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time, pool *workerPool) error {
	logger := log.With().Str("bucket", bucket).Str("prefix", cfg.SrcPrefix).Logger()

	if err := s.cfg.Destination.EnsureBucket(ctx, cfg.Destination); err != nil {
		return fmt.Errorf("ensure destination bucket %s: %w", cfg.Destination, err)
	}

	// Register the bucket (tranquila:buckets index + collection timestamp) as
	// soon as we start working on it, not only once discovery finishes
	// end-to-end. discoverFlat/discoverSharded stream and transfer objects
	// incrementally as they're listed (including burn-after-reading deletes),
	// so a large bucket can make substantial, real progress and still fail on
	// a later listing page (e.g. a transient S3 error) — that must not make it
	// vanish from `tranquila status`/ListBuckets, since nothing else in this
	// codebase reads this timestamp for sync decisions; it's purely a
	// display/existence check (internal/api's getBucket 404s when it's zero).
	if s.cfg.State != nil {
		if err := s.cfg.State.SetCollectionTime(ctx, bucket, collectionTime); err != nil {
			return fmt.Errorf("set collection time: %w", err)
		}
	}

	if s.cfg.Progress != nil {
		s.cfg.Progress.startBucket(bucket)
	}

	bucketSem := make(chan struct{}, s.maxWorkersPerBucket())

	if cfg.ShardedDiscovery {
		return s.discoverSharded(ctx, bucket, cfg, collectionTime, pool, bucketSem, logger)
	}
	err := s.discoverFlat(ctx, bucket, cfg, collectionTime, pool, bucketSem, logger)
	if err != nil && isShardableListErr(err) {
		logger.Warn().Err(err).Msg("flat listing exhausted retries with a transient error, " +
			"falling back to prefix-sharded discovery for this bucket " +
			"(set sharded-discovery: true on this bucket mapping to skip the flat attempt next time)")
		err = s.discoverSharded(ctx, bucket, cfg, collectionTime, pool, bucketSem, logger)
	}
	return err
}

// discoverObject evaluates one discovered object and, if it needs syncing,
// marks it pending and submits a transfer Job — shared by discoverFlat and
// discoverSharded so job-submission semantics never drift between the two
// discovery strategies. Blocks on bucketSem for per-bucket backpressure;
// on submission, wg.Add(1) is called before pool.submit and the job's
// OnComplete calls wg.Done() and releases bucketSem, so callers can wait for
// everything they've queued via their own wg regardless of how they batch.
// burnEligible reports whether an object last modified at modifiedAt is old
// enough to burn under minAge, evaluated against now. minAge <= 0 means no
// gate — always eligible, matching burn-after-reading's original immediate-
// delete behavior. An unknown modifiedAt (the zero time — can happen for
// watch-mode events; ObjectEvent.ModifiedAt is documented best-effort)
// refuses eligibility rather than assuming it's old enough, matching this
// codebase's established "never delete on an inconclusive answer" posture
// (see classifyHeadErr/reconcileDeletes).
func burnEligible(modifiedAt time.Time, minAge time.Duration, now time.Time) bool {
	if minAge <= 0 {
		return true
	}
	if modifiedAt.IsZero() {
		return false
	}
	return now.Sub(modifiedAt) >= minAge
}

func (s *Syncer) discoverObject(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time, obj storage.Object, pool *workerPool, bucketSem chan struct{}, wg *sync.WaitGroup, logger zerolog.Logger) (queued bool, err error) {
	needsFullSync, err := s.needsSync(ctx, bucket, obj, cfg)
	if err != nil {
		logger.Warn().Err(err).Str("key", obj.Key).Msg("state check failed, marking pending")
		needsFullSync = true
	}

	// Record that this object is still present in source, so a later
	// reconcileDeletes pass can tell "still present" apart from "no longer
	// seen" for objects needsSync otherwise never touches again. Done after
	// needsSync reads prior state, so this write doesn't change what that
	// read observed.
	if cfg.PropagateDeletes && s.cfg.State != nil {
		if err := s.cfg.State.TouchSeen(ctx, bucket, obj.Key, collectionTime); err != nil {
			logger.Warn().Err(err).Str("key", obj.Key).Msg("touch seen failed")
		}
	}

	// burnNow reflects whether THIS job should actually attempt a burn: BAR
	// enabled for the bucket AND (no min-age gate, or the object is already
	// old enough). processResults decides RemoveObject vs MarkSynced purely
	// from Job.BurnAfterReading, so this must be the final word — not
	// cfg.BurnAfterReading alone — or a deferred-for-age object would get its
	// tracking record wrongly removed despite its source never being deleted.
	burnNow := cfg.BurnAfterReading && burnEligible(obj.ModifiedAt, cfg.BurnAfterReadingMinAge, collectionTime)

	// For non-BAR buckets: skip objects that don't need sync.
	// For BAR buckets: even already-synced objects need verify-and-delete
	// (source was not deleted when the bucket was previously in normal mode).
	if !needsFullSync && !burnNow {
		if cfg.BurnAfterReading {
			logger.Debug().Str("key", obj.Key).Msg("burn-after-reading: deferring, not yet old enough")
		}
		return false, nil
	}

	verifyAndDelete := !needsFullSync // already-synced; skip re-upload, just verify+delete

	if needsFullSync && s.cfg.State != nil {
		if err := s.cfg.State.MarkPending(ctx, bucket, obj.Key, obj.ModifiedAt); err != nil {
			return false, fmt.Errorf("mark pending %s: %w", obj.Key, err)
		}
	}

	select {
	case bucketSem <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}

	wg.Add(1)
	pool.submit(Job{
		SrcBucket:  bucket,
		DstBucket:  cfg.Destination,
		Key:        obj.Key,
		DstKey:     cfg.destKey(obj.Key),
		Size:       obj.Size,
		ModifiedAt: obj.ModifiedAt,
		SrcETag:    obj.ETag,
		OnComplete: func() {
			wg.Done()
			<-bucketSem
		},
		BurnAfterReading: burnNow,
		DryRun:           s.cfg.DryRun,
		VerifyAndDelete:  verifyAndDelete,
	})
	return true, nil
}

// discoverFlat lists bucket in batches of DiscoveryBatchSize objects, waiting
// for each batch to finish syncing before fetching the next — this is what
// bounds memory/pending growth for a very large bucket, at the cost of one
// flat, bucket-wide ListObjectsV2 call per page. Returns a *storage.ListError
// (directly or wrapped) when the listing itself fails, as opposed to an error
// from onPage (mark-pending, ctx cancellation) — discoverAndSyncBucket uses
// this distinction (via isShardableListErr) to decide whether falling back to
// discoverSharded could plausibly help.
func (s *Syncer) discoverFlat(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time, pool *workerPool, bucketSem chan struct{}, logger zerolog.Logger) error {
	batchSize := s.discoveryBatchSize()
	var token *string
	var batchNum, totalCount, totalPending int

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		batchNum++
		var batchPending int
		var batchDone sync.WaitGroup

		onPage := func(page []storage.Object) error {
			var pagePending int
			for _, obj := range page {
				totalCount++
				queued, err := s.discoverObject(ctx, bucket, cfg, collectionTime, obj, pool, bucketSem, &batchDone, logger)
				if err != nil {
					return err
				}
				if queued {
					totalPending++
					batchPending++
					pagePending++
				}
			}
			if s.cfg.Progress != nil && pagePending > 0 {
				s.cfg.Progress.addPending(bucket, int64(pagePending))
			}
			return nil
		}

		discovered, nextToken, err := s.cfg.Source.ListObjectsPage(ctx, bucket, cfg.SrcPrefix, token, batchSize, onPage)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		if nextToken != nil {
			logger.Info().
				Int("batch", batchNum).
				Int("discovered", discovered).
				Int("queued", batchPending).
				Msg("batch queued, waiting for sync before continuing discovery")
			batchDone.Wait()
			logger.Info().Int("batch", batchNum).Msg("batch synced, resuming discovery")
		} else {
			logger.Info().
				Int("total", totalCount).
				Int("pending", totalPending).
				Int("batches", batchNum).
				Msg("discovery complete")
			batchDone.Wait()
		}

		token = nextToken
		if token == nil {
			return nil
		}
	}
}

// discoverSharded lists bucket via a recursive, "/"-delimited tree walk
// (storage.Client.ListObjectsTree) instead of one flat listing. Unlike
// discoverFlat, there is no linear continuation token to pause discovery on,
// so DiscoveryBatchSize's pause-while-a-batch-drains behavior does not apply
// here — jobs are submitted continuously as the tree is walked, backpressured
// by bucketSem exactly as in flat discovery, and this function waits once for
// all of them to complete after the whole tree walk finishes (mirroring
// discoverFlat's final-batch wait).
func (s *Syncer) discoverSharded(ctx context.Context, bucket string, cfg BucketConfig, collectionTime time.Time, pool *workerPool, bucketSem chan struct{}, logger zerolog.Logger) error {
	var wg sync.WaitGroup
	var totalCount, totalPending int

	onPage := func(page []storage.Object) error {
		var pagePending int
		for _, obj := range page {
			totalCount++
			queued, err := s.discoverObject(ctx, bucket, cfg, collectionTime, obj, pool, bucketSem, &wg, logger)
			if err != nil {
				return err
			}
			if queued {
				totalPending++
				pagePending++
			}
		}
		if s.cfg.Progress != nil && pagePending > 0 {
			s.cfg.Progress.addPending(bucket, int64(pagePending))
		}
		return nil
	}

	logger.Info().Msg("prefix-sharded discovery starting")
	err := s.cfg.Source.ListObjectsTree(ctx, bucket, cfg.SrcPrefix, onPage)
	wg.Wait()
	if err != nil {
		return err
	}

	logger.Info().
		Int("total", totalCount).
		Int("pending", totalPending).
		Msg("prefix-sharded discovery complete")
	return nil
}

// eventDecision is how runWatcher should handle a single watcher.ObjectEvent.
type eventDecision int

const (
	dispatchUpload      eventDecision = iota // created/modified: sync as usual
	dispatchDelete                           // removed + propagate-deletes enabled: delete destination
	dispatchSkipDelete                       // removed but propagate-deletes disabled: ignore
	dispatchOutOfPrefix                      // key outside SrcPrefix: ignore, as discovery never lists it
)

// eventDispatch decides how to handle a watcher event for its bucket, decoupled
// from job submission so the routing logic is unit-testable without a live
// worker pool.
func eventDispatch(event watcher.ObjectEvent, bc BucketConfig) eventDecision {
	// Watchers subscribe bucket-wide, so the prefix scope discovery applies must be enforced here.
	if !strings.HasPrefix(event.Key, bc.SrcPrefix) {
		return dispatchOutOfPrefix
	}
	if !event.IsDelete {
		return dispatchUpload
	}
	if !bc.PropagateDeletes {
		return dispatchSkipDelete
	}
	return dispatchDelete
}

// burnNowForEvent computes whether a live watch-mode upload event should
// attempt a burn-after-reading delete once transferred — the same age-gate
// logic discoverObject applies during flat/sharded discovery (burnEligible),
// applied here to the event's ModifiedAt instead of a listed object's.
// Decoupled from job submission for the same testability reason as
// eventDispatch.
func burnNowForEvent(event watcher.ObjectEvent, bc BucketConfig, now time.Time) bool {
	return bc.BurnAfterReading && burnEligible(event.ModifiedAt, bc.BurnAfterReadingMinAge, now)
}

// dueForReconcile reports whether enough time has passed since last for a
// propagate-deletes reconcile pass to run again. interval <= 0 means "every
// call", matching this codebase's other "0 = no throttling" flags (e.g.
// MaxWorkersPerBucket). A pure function so the boundary logic is testable
// without a real clock.
func dueForReconcile(last time.Time, interval time.Duration, now time.Time) bool {
	if interval <= 0 {
		return true
	}
	return last.IsZero() || now.Sub(last) >= interval
}

// reconcileDue checks and, if due, atomically claims this cycle's reconcile
// slot for bucket so concurrent callers don't double-run it.
func (s *Syncer) reconcileDue(bucket string, now time.Time) bool {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	if !dueForReconcile(s.lastReconcile[bucket], s.cfg.DeleteReconcileInterval, now) {
		return false
	}
	s.lastReconcile[bucket] = now
	return true
}

// reconcileVerdict is the outcome of checking a reconcile candidate against
// the source, decoupled from the HeadObject call itself so the classification
// logic is unit-testable without a live S3 client.
type reconcileVerdict int

const (
	reconcileInconclusive     reconcileVerdict = iota // transient/throttle/permanent error; retry later
	reconcileStillPresent                             // no error: object exists, listing gap not a deletion
	reconcileConfirmedDeleted                         // a genuine 404/NoSuchKey: safe to delete destination
)

// classifyHeadErr turns a source HeadObject error (nil on success) into a
// reconcileVerdict. Only a healthy negative answer (storage.ClassOK for a
// non-nil err, i.e. 404/NoSuchKey) confirms a deletion — any other error class
// is inconclusive and must not be treated as proof the object is gone.
func classifyHeadErr(err error) reconcileVerdict {
	if err == nil {
		return reconcileStillPresent
	}
	if storage.Classify(err) == storage.ClassOK {
		return reconcileConfirmedDeleted
	}
	return reconcileInconclusive
}

// reconcileDeletes finds objects tracked for bucket that were not observed in
// the most recent source listing (candidates from ScanStaleObjects) and, after
// confirming each is genuinely gone from source via HeadObject, deletes the
// corresponding destination object. A candidate still present in source (a
// listing gap, not a real deletion) is left alone and its seen_at refreshed
// rather than deleted — propagate-deletes must never delete a live object.
func (s *Syncer) reconcileDeletes(ctx context.Context, bucket string, cfg BucketConfig, before time.Time) error {
	candidates, err := s.cfg.State.ScanStaleObjects(ctx, bucket, before)
	if err != nil {
		return fmt.Errorf("scan stale objects: %w", err)
	}
	if len(candidates) == 0 {
		return nil
	}
	log.Info().Str("bucket", bucket).Int("candidates", len(candidates)).Msg("propagate-deletes: reconciling")

	var errs []error
	for _, key := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		_, _, _, headErr := s.cfg.Source.HeadObject(ctx, bucket, key)
		switch classifyHeadErr(headErr) {
		case reconcileInconclusive:
			// A transient/throttled/permanent error tells us nothing about whether
			// the source object is actually gone. Skip; re-evaluated next pass.
			log.Warn().Err(headErr).Str("bucket", bucket).Str("key", key).
				Msg("propagate-deletes: source check inconclusive, skipping candidate")
			continue
		case reconcileStillPresent:
			// A listing gap, not a deletion. Refresh seen_at so this candidate
			// isn't repeatedly re-evaluated every pass.
			log.Warn().Str("bucket", bucket).Str("key", key).
				Msg("propagate-deletes: reconcile candidate still present in source, skipping delete")
			if err := s.cfg.State.TouchSeen(ctx, bucket, key, time.Now()); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Msg("touch seen failed")
			}
			continue
		}

		job := Job{SrcBucket: bucket, DstBucket: cfg.Destination, Key: key, DstKey: cfg.destKey(key), DryRun: s.cfg.DryRun}
		if err := performDelete(ctx, job, s.cfg.Destination); err != nil {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", cfg.Destination, cfg.destKey(key), err))
			continue
		}
		if s.cfg.DryRun {
			continue
		}
		if err := s.cfg.State.RemoveObject(ctx, bucket, key); err != nil {
			errs = append(errs, fmt.Errorf("remove state %s/%s: %w", bucket, key, err))
		}
	}
	return errors.Join(errs...)
}

// ReconcileBurnAfterReading runs one burn-after-reading minimum-age reconcile
// pass across every configured bucket. Deliberately independent of
// propagate-deletes' reconcileDeletes (separate schedule, separate Redis
// scan, no HeadObject still-present check — an aged object was never
// "expected gone," it's simply now old enough) — see
// RunBurnAfterReadingReconciler for why this needs its own trigger, decoupled
// from --watch-mode/--watch-interval. Exported: called directly once for
// one-shot mode, and once per tick by RunBurnAfterReadingReconciler.
func (s *Syncer) ReconcileBurnAfterReading(ctx context.Context) error {
	bucketMap, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	var errs []error
	for bucket, cfg := range bucketMap {
		if !cfg.BurnAfterReading || cfg.BurnAfterReadingMinAge <= 0 {
			continue
		}
		if err := s.reconcileBurnAfterReadingBucket(ctx, bucket, cfg, now); err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, fmt.Errorf("reconcile burn-after-reading %s: %w", bucket, err))
		}
	}
	return errors.Join(errs...)
}

// reconcileBurnAfterReadingBucket finds synced objects in bucket whose source
// has aged past cfg.BurnAfterReadingMinAge and, after verifying each against
// the destination, deletes the source. Mirrors reconcileDeletes' shape
// (candidates → per-candidate action → remove state, errors.Join across
// failures) but calls performVerifyAndDelete instead of performDelete: no
// SrcETag is stored in Redis for a reconcile candidate, so it falls back to
// performVerifyAndDelete's own content-hash tier automatically, the same as
// any other caller missing an ETag.
func (s *Syncer) reconcileBurnAfterReadingBucket(ctx context.Context, bucket string, cfg BucketConfig, now time.Time) error {
	candidates, err := s.cfg.State.ScanAgedSyncedObjects(ctx, bucket, now.Add(-cfg.BurnAfterReadingMinAge))
	if err != nil {
		return fmt.Errorf("scan aged objects: %w", err)
	}
	if len(candidates) == 0 {
		return nil
	}
	log.Info().Str("bucket", bucket).Int("candidates", len(candidates)).Msg("burn-after-reading: reconciling aged objects")

	var errs []error
	for _, key := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job := Job{SrcBucket: bucket, DstBucket: cfg.Destination, Key: key, DstKey: cfg.destKey(key), DryRun: s.cfg.DryRun}
		if err := performVerifyAndDelete(ctx, job, s.cfg.Destination, s.cfg.Source); err != nil {
			errs = append(errs, fmt.Errorf("verify and delete %s/%s: %w", bucket, key, err))
			continue
		}
		if s.cfg.DryRun {
			continue
		}
		if err := s.cfg.State.RemoveObject(ctx, bucket, key); err != nil {
			errs = append(errs, fmt.Errorf("remove state %s/%s: %w", bucket, key, err))
		}
	}
	return errors.Join(errs...)
}

// RunBurnAfterReadingReconciler loops ReconcileBurnAfterReading every interval
// until ctx is cancelled, running the first pass immediately. Deliberately
// independent of RunWatch/RunWatcher's own cycle — the same background loop
// runs unconditionally for all three --watch-mode backends (poll included),
// since burn-after-reading age-gating and the regular sync/discovery cadence
// are orthogonal concerns with very different natural frequencies: a sync
// cycle might run every few seconds, but checking for month-old objects to
// burn does not need to, and in pure event-driven (minio/sqs) watch mode
// there is no other periodic re-scan at all to piggyback on (unlike
// propagate-deletes' reconcileDeletes, which only gets a one-time pass at
// RunWatcher startup for those backends). A failed pass is logged, not
// returned — a transient failure must not kill a reconciler meant to run for
// the life of the process.
func (s *Syncer) RunBurnAfterReadingReconciler(ctx context.Context, interval time.Duration) error {
	for {
		if err := s.ReconcileBurnAfterReading(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("burn-after-reading: reconcile pass failed")
		}
		if !waitOrDone(ctx, interval) {
			return nil
		}
	}
}

func (s *Syncer) resolveBuckets(ctx context.Context) (map[string]BucketConfig, error) {
	if len(s.cfg.Buckets) > 0 {
		return s.cfg.Buckets, nil
	}
	discovered, err := s.cfg.Source.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list source buckets: %w", err)
	}
	m := make(map[string]BucketConfig, len(discovered))
	for _, b := range discovered {
		m[b] = BucketConfig{Destination: s.cfg.DestBucketPrefix + b}
	}
	return m, nil
}

func (s *Syncer) needsSync(ctx context.Context, bucket string, obj storage.Object, cfg BucketConfig) (bool, error) {
	stored, err := s.cfg.State.GetObject(ctx, bucket, obj.Key)
	if err != nil {
		return false, err
	}
	if stored == nil {
		return true, nil
	}
	if stored.Status == state.StatusPending || stored.Status == state.StatusFailed {
		return true, nil
	}
	// Re-sync if source object was modified after the stored modification time.
	if obj.ModifiedAt.After(stored.ModifiedAt) {
		return true, nil
	}
	// Optionally verify destination size matches source to catch incomplete uploads.
	if s.cfg.CheckSizes && obj.Size > 0 {
		dstSize, _, _, err := s.cfg.Destination.HeadObject(ctx, cfg.Destination, cfg.destKey(obj.Key))
		if err != nil {
			// Object missing or inaccessible on destination — re-sync.
			return true, nil
		}
		if dstSize != obj.Size {
			return true, nil
		}
	}
	return false, nil
}

// logStateWriteErr logs a failed Redis bookkeeping write. These are never
// fatal to the cycle — the transfer itself already succeeded or failed on its
// own terms — but silently discarding them (as processResults previously did)
// leaves no trace of a Redis outage beyond go-redis's own low-level pool logs,
// which say nothing about whether tranquila's calls are still failing at any
// given moment.
func logStateWriteErr(op, bucket, key string, err error) {
	if err == nil {
		return
	}
	log.Warn().Err(err).Str("op", op).Str("bucket", bucket).Str("key", key).Msg("state write failed")
}

func (s *Syncer) processResults(ctx context.Context, results <-chan Result) {
	for r := range results {
		attrs := []attribute.KeyValue{attribute.String("bucket", r.Job.SrcBucket)}
		if r.Job.IsDelete {
			if r.Err != nil {
				// Leave the tracking record untouched (rather than MarkFailed) so a
				// future reconcile pass or restart's initial sync can retry the
				// propagated delete; MarkFailed would make needsSync retry an upload
				// of a source object that no longer exists.
				log.Error().
					Err(r.Err).
					Str("bucket", r.Job.SrcBucket).
					Str("key", r.Job.Key).
					Msg("propagate-deletes: destination delete failed")
				s.m.failed.Add(ctx, 1, metric.WithAttributes(attrs...))
			} else {
				logStateWriteErr("remove_object", r.Job.SrcBucket, r.Job.Key, s.cfg.State.RemoveObject(ctx, r.Job.SrcBucket, r.Job.Key))
				s.m.synced.Add(ctx, 1, metric.WithAttributes(attrs...))
			}
			if r.Job.OnComplete != nil {
				r.Job.OnComplete()
			}
			continue
		}
		if r.Err != nil {
			ev := log.Error().
				Err(r.Err).
				Str("bucket", r.Job.SrcBucket).
				Str("key", r.Job.Key)
			var mismatch *verifyMismatchError
			if errors.As(r.Err, &mismatch) {
				ev = ev.Str("verify_method", mismatch.Method).
					Str("source_value", mismatch.Source).
					Str("dest_value", mismatch.Destination)
			}
			ev.Msg("transfer failed")
			logStateWriteErr("mark_failed", r.Job.SrcBucket, r.Job.Key, s.cfg.State.MarkFailed(ctx, r.Job.SrcBucket, r.Job.Key))
			s.m.failed.Add(ctx, 1, metric.WithAttributes(attrs...))
			if s.cfg.Progress != nil {
				s.cfg.Progress.recordFailed(r.Job.SrcBucket)
			}
		} else {
			log.Debug().
				Str("bucket", r.Job.SrcBucket).
				Str("key", r.Job.Key).
				Str("size", humanize.Bytes(uint64(r.Job.Size))).
				Dur("duration", r.Duration).
				Msg("transfer complete")
			// BAR (non-dry-run): source was deleted — remove the tracking record so
			// BucketStats stays accurate and future runs don't encounter stale entries.
			if r.Job.BurnAfterReading && !r.Job.DryRun {
				logStateWriteErr("remove_object", r.Job.SrcBucket, r.Job.Key, s.cfg.State.RemoveObject(ctx, r.Job.SrcBucket, r.Job.Key))
			} else {
				logStateWriteErr("mark_synced", r.Job.SrcBucket, r.Job.Key, s.cfg.State.MarkSynced(ctx, r.Job.SrcBucket, r.Job.Key))
			}
			s.m.synced.Add(ctx, 1, metric.WithAttributes(attrs...))
			s.m.bytesTransferred.Add(ctx, r.Job.Size, metric.WithAttributes(attrs...))
			s.m.duration.Record(ctx, r.Duration.Seconds(), metric.WithAttributes(attrs...))
			if s.cfg.Progress != nil {
				s.cfg.Progress.recordSynced(r.Job.SrcBucket)
			}
		}
		if r.Job.OnComplete != nil {
			r.Job.OnComplete()
		}
	}
}

// RunWatch repeatedly calls Run until ctx is cancelled, sleeping interval between
// each completed cycle. A transient failure is retried with exponential backoff
// rather than aborting: watch mode is a long-lived service and must survive a
// flaky endpoint without exiting into a crash-loop. Misconfiguration still returns.
func (s *Syncer) RunWatch(ctx context.Context, interval time.Duration) error {
	return s.runWatch(ctx, interval, s.Run, waitOrDone)
}

// runWatch is the testable core of RunWatch; cycleFn replaces s.Run and sleep
// replaces waitOrDone so tests can inject controlled behaviour without requiring
// real S3 or Redis connections, or real delays.
func (s *Syncer) runWatch(ctx context.Context, interval time.Duration, cycleFn func(context.Context) error, sleep sleeper) error {
	var fails int
	for {
		err := cycleFn(ctx)
		switch {
		case err == nil || errors.Is(err, context.Canceled):
			if fails > 0 {
				log.Info().Int("after_failures", fails).Msg("watch: cycle recovered")
				fails = 0
			}
		case isFatalCycleErr(err):
			return err
		default:
			fails++
			delay := s.cycleBackoff(fails)
			s.m.cycleFailures.Add(ctx, 1)
			log.Error().Err(err).Int("consecutive_failures", fails).Dur("retry_in", delay).
				Msg("watch: cycle failed, backing off")
			if !sleep(ctx, delay) {
				return nil
			}
			continue
		}
		log.Info().Dur("interval", interval).Msg("watch: cycle complete, waiting before next discovery")
		if !sleep(ctx, interval) {
			return nil
		}
	}
}

// RunWatcher consumes events from w while a full catch-up sync — for changes
// missed while the program was down — runs alongside it, not before it (see
// runWatcherWithCatchUp). In-flight transfers complete before returning on
// context cancellation.
func (s *Syncer) RunWatcher(ctx context.Context, w watcher.Watcher) error {
	bucketMap, err := s.resolveBuckets(ctx)
	if err != nil {
		return err
	}

	for _, bc := range bucketMap {
		if err := s.cfg.Destination.EnsureBucket(ctx, bc.Destination); err != nil {
			log.Error().Err(err).Str("bucket", bc.Destination).Msg("ensure destination bucket failed")
		}
	}

	srcBuckets := make([]string, 0, len(bucketMap))
	for b := range bucketMap {
		srcBuckets = append(srcBuckets, b)
	}

	return s.runWatcherWithCatchUp(ctx, w, srcBuckets, bucketMap, s.Run, waitOrDone)
}

// runWatcherWithCatchUp runs the catch-up sync and the event loop concurrently.
// The catch-up used to run to completion first, which meant a single bucket
// whose discovery never succeeds (a very large bucket whose listing keeps
// timing out) kept live events from *every* bucket from being consumed at all,
// indefinitely — initialSync only returns once a whole cycle succeeds.
// cycleFn/sleep are injected so this is testable without real S3 or Redis.
func (s *Syncer) runWatcherWithCatchUp(ctx context.Context, w watcher.Watcher, srcBuckets []string, bucketMap map[string]BucketConfig, cycleFn func(context.Context) error, sleep sleeper) error {
	// Own cancel so a fatal (all-permanent, i.e. misconfigured) catch-up stops
	// the event loop too instead of leaving it running until SIGTERM.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var catchUpErr, watchErr error

	wg.Go(func() {
		catchUpErr = s.initialSync(ctx, cycleFn, sleep)
		if catchUpErr != nil {
			cancel()
		}
	})

	wg.Go(func() {
		// The event stream can close without error (a MinIO notification stream
		// that drops is not reconnected by the watcher). Returning here would exit
		// 0 and let K8s restart the pod, so reconnect with the same backoff instead.
		for n := 1; ; n++ {
			err := s.runWatcher(ctx, w, srcBuckets, bucketMap)
			if err != nil || ctx.Err() != nil {
				watchErr = err
				return
			}
			delay := s.cycleBackoff(n)
			log.Warn().Int("attempt", n).Dur("retry_in", delay).Msg("watch: event stream closed, reconnecting")
			if !sleep(ctx, delay) {
				return
			}
		}
	})

	wg.Wait()
	return errors.Join(watchErr, catchUpErr)
}

// initialSync retries the catch-up cycle with backoff so a gateway that is flaky
// at startup does not kill an event-driven watcher before it reaches its event loop.
func (s *Syncer) initialSync(ctx context.Context, cycleFn func(context.Context) error, sleep sleeper) error {
	for n := 1; ; n++ {
		err := cycleFn(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		if isFatalCycleErr(err) {
			return err
		}
		delay := s.cycleBackoff(n)
		s.m.cycleFailures.Add(ctx, 1)
		log.Error().Err(err).Int("attempt", n).Dur("retry_in", delay).
			Msg("watch: initial sync failed, backing off")
		if !sleep(ctx, delay) {
			return nil
		}
	}
}

// runWatcher is the testable event-loop core of RunWatcher; it accepts a pre-resolved
// bucket map and an already-started Watcher so tests can inject controlled behaviour.
func (s *Syncer) runWatcher(ctx context.Context, w watcher.Watcher, srcBuckets []string, bucketMap map[string]BucketConfig) error {
	events, err := w.Watch(ctx, srcBuckets)
	if err != nil {
		return fmt.Errorf("start watcher: %w", err)
	}

	pool := newWorkerPool(ctx, s.cfg.Workers, s.transfer, s.m.activeWorkers)

	var resultWg sync.WaitGroup
	resultWg.Go(func() {
		s.processResults(ctx, pool.resultsCh())
	})

	log.Info().Strs("buckets", srcBuckets).Msg("watch: listening for object events")

	for event := range events {
		bc, ok := bucketMap[event.Bucket]
		if !ok {
			log.Warn().Str("bucket", event.Bucket).Msg("watch: received event for unknown bucket, skipping")
			continue
		}

		switch eventDispatch(event, bc) {
		case dispatchOutOfPrefix:
			log.Debug().Str("bucket", event.Bucket).Str("key", event.Key).Str("prefix", bc.SrcPrefix).
				Msg("watch: event outside configured source prefix, ignoring")
			continue
		case dispatchSkipDelete:
			log.Debug().Str("bucket", event.Bucket).Str("key", event.Key).
				Msg("watch: source deletion event, propagate-deletes disabled for bucket, ignoring")
			continue
		case dispatchDelete:
			pool.submit(Job{
				SrcBucket: event.Bucket,
				DstBucket: bc.Destination,
				Key:       event.Key,
				DstKey:    bc.destKey(event.Key),
				IsDelete:  true,
				DryRun:    s.cfg.DryRun,
			})
			continue
		}

		if s.cfg.State != nil {
			if err := s.cfg.State.MarkPending(ctx, event.Bucket, event.Key, event.ModifiedAt); err != nil {
				log.Error().Err(err).Str("bucket", event.Bucket).Str("key", event.Key).Msg("watch: mark pending failed")
				continue
			}
		}
		burnNow := burnNowForEvent(event, bc, time.Now().UTC())
		pool.submit(Job{
			SrcBucket:        event.Bucket,
			DstBucket:        bc.Destination,
			Key:              event.Key,
			DstKey:           bc.destKey(event.Key),
			Size:             event.Size,
			ModifiedAt:       event.ModifiedAt,
			BurnAfterReading: burnNow,
			DryRun:           s.cfg.DryRun,
		})
	}

	pool.close()
	resultWg.Wait()
	return nil
}

func (s *Syncer) transfer(ctx context.Context, job Job) error {
	if job.IsDelete {
		return performDelete(ctx, job, s.cfg.Destination)
	}

	// VerifyAndDelete: object was already synced before BAR mode was enabled.
	// Skip re-upload; confirm destination content matches source (checksum), then delete source.
	if job.VerifyAndDelete {
		return performVerifyAndDelete(ctx, job, s.cfg.Destination, s.cfg.Source)
	}

	body, srcSize, err := s.cfg.Source.GetObject(ctx, job.SrcBucket, job.Key)
	if err != nil {
		return err
	}
	defer body.Close()

	job.Size = srcSize
	uploadCRC32, err := s.cfg.Destination.PutObject(ctx, job.DstBucket, job.DstKey, body, srcSize)
	if err != nil {
		return err
	}

	// Verify destination size and capture stored CRC32/ETag for burn-after-reading.
	// Skip size check when srcSize is unknown (server did not provide Content-Length).
	dstSize, storedCRC32, dstETag, err := s.cfg.Destination.HeadObject(ctx, job.DstBucket, job.DstKey)
	if err != nil {
		return fmt.Errorf("verify %s/%s: %w", job.DstBucket, job.DstKey, err)
	}
	if srcSize > 0 && dstSize != srcSize {
		return fmt.Errorf("size mismatch for %s/%s: source=%d destination=%d",
			job.DstBucket, job.DstKey, srcSize, dstSize)
	}

	if !job.BurnAfterReading {
		return nil
	}
	return performBurnAfterReading(ctx, job, s.cfg.Source, s.cfg.Destination, uploadCRC32, storedCRC32, dstETag)
}

// objectDeleter is the narrow interface that performBurnAfterReading needs from the source client.
type objectDeleter interface {
	DeleteObject(ctx context.Context, bucket, key string) error
}

// verifyMismatchError reports that source and destination content did not
// match during burn-after-reading/verify-and-delete verification. The compared
// values are structured fields rather than baked into the error string, so a
// caller logging this error (see processResults) can attach them as their own
// log fields instead of parsing them back out of free text.
type verifyMismatchError struct {
	Bucket, Key string
	Method      string // "crc32" | "etag" | "content"
	Source      string
	Destination string
}

func (e *verifyMismatchError) Error() string {
	return fmt.Sprintf("burn-after-reading: %s mismatch for %s/%s, refusing to delete source", e.Method, e.Bucket, e.Key)
}

// objectGetter is the narrow interface needed to read an object's content, so its
// checksum can be computed rather than trusted from possibly-absent S3 metadata.
type objectGetter interface {
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error)
}

// destinationVerifier is the narrow interface that performVerifyAndDelete needs from the destination client.
type destinationVerifier interface {
	objectGetter
	HeadObject(ctx context.Context, bucket, key string) (size int64, checksumCRC32, etag string, err error)
}

// sourceReadDeleter is the narrow interface that performVerifyAndDelete needs from the source
// client: read (to checksum) and delete.
type sourceReadDeleter interface {
	objectGetter
	DeleteObject(ctx context.Context, bucket, key string) error
}

// crc32Checksum streams an object's full content through CRC32 (IEEE) and returns
// the result as a lowercase hex string. Used to compare source and destination
// content directly, rather than relying on S3-reported checksum metadata: an
// object's own upload path may not have stored one (e.g. the source in
// verify-and-delete was uploaded by whatever put it there originally, not by
// tranquila), and a stored composite checksum for a multipart object is not
// comparable to a full-object checksum computed the same way on both sides.
func crc32Checksum(ctx context.Context, g objectGetter, bucket, key string) (string, error) {
	body, _, err := g.GetObject(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	defer body.Close()
	h := crc32.NewIEEE()
	if _, err := io.Copy(h, body); err != nil {
		return "", err
	}
	return fmt.Sprintf("%08x", h.Sum32()), nil
}

// performVerifyAndDelete handles the verify-and-delete path for objects that were already synced
// before burn-after-reading mode was enabled. No re-upload is performed; instead it confirms the
// destination still holds the object (size check) and that its content matches the source, then
// deletes from source.
//
// Content is compared via ETag when possible: identical single-part uploads produce identical
// ETags (S3's MD5-of-content convention) on any S3-compatible backend, at zero read cost — the
// source's ETag comes from discovery listing, the destination's from the HeadObject call the size
// check already makes. A multipart ETag is a composite of its parts' MD5s plus a "-partCount"
// suffix and is never comparable this way, so that case (and any other unparseable ETag) falls
// back to downloading and hashing both objects' full content — the cost of verifying an
// irreversible delete when neither side is guaranteed to carry a usable stored checksum.
func performVerifyAndDelete(ctx context.Context, job Job, dst destinationVerifier, src sourceReadDeleter) error {
	dstSize, _, dstETag, err := dst.HeadObject(ctx, job.DstBucket, job.DstKey)
	if err != nil {
		return fmt.Errorf("burn-after-reading verify: destination check %s/%s: %w", job.DstBucket, job.DstKey, err)
	}
	if job.Size > 0 && dstSize != job.Size {
		return fmt.Errorf("burn-after-reading verify: size mismatch %s/%s: expected=%d got=%d",
			job.DstBucket, job.DstKey, job.Size, dstSize)
	}

	// refuseMismatch reports a verification mismatch as a structured error, or —
	// during a dry run — logs the same fields and lets the cycle continue rather
	// than failing it, matching performBurnAfterReading's dry-run behavior.
	refuseMismatch := func(method, srcVal, dstVal string) error {
		if job.DryRun {
			log.Warn().
				Str("bucket", job.SrcBucket).Str("key", job.Key).
				Str("verify_method", method).Str("source_value", srcVal).Str("dest_value", dstVal).
				Msg("burn-after-reading: DRY-RUN would refuse to delete source object (verification failed)")
			return nil
		}
		return &verifyMismatchError{Bucket: job.SrcBucket, Key: job.Key, Method: method, Source: srcVal, Destination: dstVal}
	}

	verifiedBy := "content"
	verified := false
	if srcMD5, srcOK := storage.SinglePartMD5(job.SrcETag); srcOK {
		if dstMD5, dstOK := storage.SinglePartMD5(dstETag); dstOK {
			if srcMD5 != dstMD5 {
				return refuseMismatch("etag", srcMD5, dstMD5)
			}
			verifiedBy, verified = "etag", true
		}
	}
	if !verified {
		srcChecksum, err := crc32Checksum(ctx, src, job.SrcBucket, job.Key)
		if err != nil {
			return fmt.Errorf("burn-after-reading verify: read source %s/%s: %w", job.SrcBucket, job.Key, err)
		}
		dstChecksum, err := crc32Checksum(ctx, dst, job.DstBucket, job.DstKey)
		if err != nil {
			return fmt.Errorf("burn-after-reading verify: read destination %s/%s: %w", job.DstBucket, job.DstKey, err)
		}
		if srcChecksum != dstChecksum {
			return refuseMismatch("content", srcChecksum, dstChecksum)
		}
	}

	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Int64("size", dstSize).
		Str("verified_by", verifiedBy).
		Msg("burn-after-reading: destination verified")
	if job.DryRun {
		log.Info().
			Str("bucket", job.SrcBucket).
			Str("key", job.Key).
			Msg("burn-after-reading: DRY-RUN would delete source object")
		return nil
	}
	if err := src.DeleteObject(ctx, job.SrcBucket, job.Key); err != nil {
		return fmt.Errorf("burn-after-reading: delete source %s/%s: %w", job.SrcBucket, job.Key, err)
	}
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Msg("burn-after-reading: source object deleted")
	return nil
}

// performDelete deletes the destination object mirroring a source deletion
// (propagate-deletes). Unlike burn-after-reading, no verification is needed
// here beyond what already gated the job: an event-driven delete came from a
// watcher notification, and a poll-mode reconciliation delete was already
// verified against the source via HeadObject before this job was submitted.
func performDelete(ctx context.Context, job Job, dst objectDeleter) error {
	if job.DryRun {
		log.Info().
			Str("bucket", job.DstBucket).
			Str("key", job.DstKey).
			Msg("propagate-deletes: DRY-RUN would delete destination object")
		return nil
	}
	if err := dst.DeleteObject(ctx, job.DstBucket, job.DstKey); err != nil {
		return fmt.Errorf("propagate-deletes: delete destination %s/%s: %w", job.DstBucket, job.DstKey, err)
	}
	log.Info().
		Str("bucket", job.DstBucket).
		Str("key", job.DstKey).
		Msg("propagate-deletes: destination object deleted")
	return nil
}

// performBurnAfterReading verifies destination integrity and deletes the source object
// for one just uploaded in this run. The upload/stored CRC32 pair (from S3's own
// computation: the upload response and the stored value from HeadObject) is the
// cheapest check and is preferred when available, but not every S3-compatible
// endpoint echoes flexible checksums — when either is empty, this falls back to
// the same ETag-then-content-hash tiers performVerifyAndDelete uses, rather than
// refusing outright. Safe-by-default: content is never assumed to match without
// having verified it via at least one of these tiers.
func performBurnAfterReading(ctx context.Context, job Job, src sourceReadDeleter, dst objectGetter, uploadCRC32, storedCRC32, dstETag string) error {
	// refuseMismatch reports a verification mismatch as a structured error, or —
	// during a dry run — logs the same fields and lets the cycle continue rather
	// than failing it.
	refuseMismatch := func(method, srcVal, dstVal string) error {
		if job.DryRun {
			log.Warn().
				Str("bucket", job.SrcBucket).Str("key", job.Key).
				Str("verify_method", method).Str("source_value", srcVal).Str("dest_value", dstVal).
				Msg("burn-after-reading: DRY-RUN would refuse to delete source object (verification failed)")
			return nil
		}
		return &verifyMismatchError{Bucket: job.SrcBucket, Key: job.Key, Method: method, Source: srcVal, Destination: dstVal}
	}

	verifiedBy := ""
	switch {
	case uploadCRC32 != "" && storedCRC32 != "":
		if uploadCRC32 != storedCRC32 {
			return refuseMismatch("crc32", uploadCRC32, storedCRC32)
		}
		verifiedBy = "crc32"
	default:
		if srcMD5, srcOK := storage.SinglePartMD5(job.SrcETag); srcOK {
			if dstMD5, dstOK := storage.SinglePartMD5(dstETag); dstOK {
				if srcMD5 != dstMD5 {
					return refuseMismatch("etag", srcMD5, dstMD5)
				}
				verifiedBy = "etag"
			}
		}
		if verifiedBy == "" {
			srcChecksum, err := crc32Checksum(ctx, src, job.SrcBucket, job.Key)
			if err != nil {
				return fmt.Errorf("burn-after-reading: read source %s/%s: %w", job.SrcBucket, job.Key, err)
			}
			dstChecksum, err := crc32Checksum(ctx, dst, job.DstBucket, job.DstKey)
			if err != nil {
				return fmt.Errorf("burn-after-reading: read destination %s/%s: %w", job.DstBucket, job.DstKey, err)
			}
			if srcChecksum != dstChecksum {
				return refuseMismatch("content", srcChecksum, dstChecksum)
			}
			verifiedBy = "content"
		}
	}

	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Str("verified_by", verifiedBy).
		Msg("burn-after-reading: verified")
	if job.DryRun {
		log.Info().
			Str("bucket", job.SrcBucket).
			Str("key", job.Key).
			Msg("burn-after-reading: DRY-RUN would delete source object")
		return nil
	}
	if err := src.DeleteObject(ctx, job.SrcBucket, job.Key); err != nil {
		return fmt.Errorf("burn-after-reading: delete source %s/%s: %w", job.SrcBucket, job.Key, err)
	}
	log.Info().
		Str("bucket", job.SrcBucket).
		Str("key", job.Key).
		Msg("burn-after-reading: source object deleted")
	return nil
}
