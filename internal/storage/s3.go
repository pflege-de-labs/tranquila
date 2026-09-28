package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/time/rate"
)

type Config struct {
	Endpoint      string
	Region        string
	AccessKey     string
	SecretKey     string
	RateLimit     float64      // max S3 API calls/sec for this client; 0 = unlimited
	FailThreshold int          // consecutive transient failures before the rate is halved (0 = default 5)
	Name          string       // "source"/"destination"; labels metrics
	Meter         metric.Meter // optional; zero value produces no-op instruments

	// ListAttemptTimeout bounds a single ListObjectsV2 attempt (0 = default
	// defaultListAttemptTimeout). Only meaningful on the source client — nothing
	// calls ListObjectsPage/ListObjectsTree on the destination.
	ListAttemptTimeout time.Duration
	// ShardedDiscoveryConcurrency bounds how many prefixes are listed
	// concurrently during a sharded discovery tree walk (0 = default
	// defaultShardedDiscoveryConcurrency). Only meaningful on the source client.
	ShardedDiscoveryConcurrency int
	// DiscoveryCheckpoints persists a per-prefix resume point during a sharded
	// discovery walk (nil = disabled, every prefix lists from its first page on
	// every cycle). Only meaningful on the source client.
	DiscoveryCheckpoints DiscoveryCheckpointer
	// DiscoveryPrefixBudget bounds how long one prefix may be listed in a single
	// sharded walk before it yields its worker slot (0 = default
	// defaultDiscoveryPrefixBudget, negative = unbounded). Only meaningful on
	// the source client.
	DiscoveryPrefixBudget time.Duration
	// ListRetryBudget bounds how long listPageWithRetry may keep retrying one
	// page's ListObjectsV2 call, including escalated per-attempt deadlines
	// (0 = default defaultListRetryBudget, negative = unbounded). Distinct
	// scope from DiscoveryPrefixBudget: this bounds one page's retries;
	// DiscoveryPrefixBudget bounds a whole prefix's multi-page walk. Only
	// meaningful on the source client.
	ListRetryBudget time.Duration
}

type Object struct {
	Bucket     string
	Key        string
	ModifiedAt time.Time
	Size       int64
	ETag       string
}

type clientMetrics struct {
	opDuration   metric.Float64Histogram
	errors       metric.Int64Counter
	limitChanges metric.Int64Counter
	attrs        []attribute.KeyValue // cached endpoint label
}

type Client struct {
	s3      *s3.Client
	tm      *transfermanager.Client
	region  string
	limiter *rate.Limiter // never nil; rate.Inf = unlimited, mutated only via aimd
	aimd    *aimd
	m       clientMetrics

	listAttemptTimeout          time.Duration
	shardedDiscoveryConcurrency int
	checkpoints                 DiscoveryCheckpointer // nil = checkpointing disabled
	discoveryPrefixBudget       time.Duration
	listRetryBudget             time.Duration

	// parkedMu guards parkedPrefixes: sharded discovery for several buckets can
	// run concurrently on one Client, and ListObjectsTree writes it from
	// whichever goroutine called it while the metrics callback and
	// ParkedPrefixes read it from others.
	parkedMu       sync.Mutex
	parkedPrefixes map[string]int64 // bucket -> count as of its last completed cycle
}

func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// The SDK default of 3 attempts is not enough to ride out a gateway that
	// returns 504 under load. Applies to every operation, including the ones
	// with no retry wrapper of their own (Get/Put/Head/Delete, EnsureBucket).
	awsCfg.Retryer = func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), s3MaxAttempts)
	}

	clientOpts := []func(*s3.Options){
		// The SDK's own default stderr logger bypasses our zerolog pipeline
		// entirely, with no bucket/key/request context, and warns on every
		// GetObject response lacking an SDK-recognized checksum header —
		// benign and expected against non-AWS S3-compatible backends,
		// tranquila's primary target, which commonly don't echo one. Not
		// load-bearing: tranquila does its own multi-tier verification
		// elsewhere (PutObject's CRC32, HeadObject's ChecksumMode,
		// performVerifyAndDelete's tiered CRC32->ETag->content-hash fallback).
		func(o *s3.Options) { o.DisableLogOutputChecksumValidationSkipped = true },
	}
	if cfg.Endpoint != "" {
		endpoint := cfg.Endpoint
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3c := s3.NewFromConfig(awsCfg, clientOpts...)

	// Always construct the limiter so the pointer is never nil and never
	// swapped: rate.Inf short-circuits Wait, and only SetLimit ever mutates it.
	base := rate.Inf
	if cfg.RateLimit > 0 {
		base = rate.Limit(cfg.RateLimit)
	}
	// Burst of 1 enforces strict per-call pacing with no token accumulation.
	lim := rate.NewLimiter(base, 1)

	listAttemptTimeout := cfg.ListAttemptTimeout
	if listAttemptTimeout <= 0 {
		listAttemptTimeout = defaultListAttemptTimeout
	}
	shardedDiscoveryConcurrency := cfg.ShardedDiscoveryConcurrency
	if shardedDiscoveryConcurrency <= 0 {
		shardedDiscoveryConcurrency = defaultShardedDiscoveryConcurrency
	}
	// Negative means "no budget"; zero means "use the default", matching the
	// 0-is-the-default convention of the flags around it.
	discoveryPrefixBudget := cfg.DiscoveryPrefixBudget
	if discoveryPrefixBudget == 0 {
		discoveryPrefixBudget = defaultDiscoveryPrefixBudget
	}
	// Same convention as discoveryPrefixBudget above.
	listRetryBudget := cfg.ListRetryBudget
	if listRetryBudget == 0 {
		listRetryBudget = defaultListRetryBudget
	}

	c := &Client{
		s3:                          s3c,
		tm:                          transfermanager.New(s3c),
		region:                      cfg.Region,
		limiter:                     lim,
		aimd:                        newAIMD(lim, base, cfg.FailThreshold),
		listAttemptTimeout:          listAttemptTimeout,
		shardedDiscoveryConcurrency: shardedDiscoveryConcurrency,
		checkpoints:                 cfg.DiscoveryCheckpoints,
		discoveryPrefixBudget:       discoveryPrefixBudget,
		listRetryBudget:             listRetryBudget,
		parkedPrefixes:              make(map[string]int64),
	}
	if err := c.initMetrics(cfg); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) initMetrics(cfg Config) error {
	name := cfg.Name
	if name == "" {
		name = "s3"
	}
	m := clientMetrics{attrs: []attribute.KeyValue{attribute.String("endpoint", name)}}

	// metric.Meter is an interface, so its zero value is nil rather than a no-op.
	meter := cfg.Meter
	if meter == nil {
		meter = noop.Meter{}
	}

	var err error
	if m.opDuration, err = meter.Float64Histogram("tranquila.s3.operation.duration",
		metric.WithDescription("Duration of individual S3 API calls"),
		metric.WithUnit("ms")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if m.errors, err = meter.Int64Counter("tranquila.s3.errors",
		metric.WithDescription("S3 API call failures by class")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if m.limitChanges, err = meter.Int64Counter("tranquila.s3.rate_limit.changes",
		metric.WithDescription("Rate limit adjustments made by congestion control")); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if _, err = meter.Float64ObservableGauge("tranquila.s3.rate_limit",
		metric.WithDescription("Effective S3 API call rate limit; 0 = unlimited"),
		metric.WithUnit("{call}/s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(c.aimd.state().Current, metric.WithAttributes(m.attrs...))
			return nil
		})); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	if _, err = meter.Int64ObservableGauge("tranquila.s3.rate_limit.degraded",
		metric.WithDescription("1 while the endpoint's rate limit is reduced by congestion control"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			var v int64
			if c.aimd.state().Degraded {
				v = 1
			}
			o.Observe(v, metric.WithAttributes(m.attrs...))
			return nil
		})); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}
	// Source-only in practice — nothing lists the destination, so its
	// parkedPrefixes map stays empty and this reports nothing for it.
	if _, err = meter.Int64ObservableGauge("tranquila.s3.discovery.parked_prefixes",
		metric.WithDescription("Sharded-discovery prefixes resumed onto a checkpointed page the backend still could not answer, per bucket, as of the most recently completed cycle"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			c.parkedMu.Lock()
			defer c.parkedMu.Unlock()
			for bucket, n := range c.parkedPrefixes {
				o.Observe(n, metric.WithAttributes(attribute.String("endpoint", name), attribute.String("bucket", bucket)))
			}
			return nil
		})); err != nil {
		return fmt.Errorf("init s3 metrics: %w", err)
	}

	c.m = m
	return nil
}

// wait blocks until the rate limiter allows the next S3 API call.
// Returns immediately while the limit is rate.Inf (unlimited).
func (c *Client) wait(ctx context.Context) error {
	return c.limiter.Wait(ctx)
}

// recordOp records a completed S3 API call with operation name, bucket, success flag, and duration.
func (c *Client) recordOp(ctx context.Context, op, bucket string, start time.Time, err error) {
	c.recordOpClass(ctx, op, bucket, start, err, Classify(err))
}

// recordOpClass is recordOp with an explicit congestion class, for callers
// whose own context deadline makes Classify's reading of the error wrong.
func (c *Client) recordOpClass(ctx context.Context, op, bucket string, start time.Time, err error, class ErrClass) {
	status := "ok"
	if err != nil {
		status = "error"
	}
	c.m.opDuration.Record(ctx, float64(time.Since(start).Milliseconds()),
		metric.WithAttributes(
			attribute.String("operation", op),
			attribute.String("bucket", bucket),
			attribute.String("status", status),
		))
	c.observe(ctx, class)
}

// observe feeds one call outcome to the endpoint's congestion controller.
func (c *Client) observe(ctx context.Context, class ErrClass) {
	if class != ClassOK {
		c.m.errors.Add(ctx, 1, metric.WithAttributes(
			append(c.m.attrs, attribute.String("class", class.String()))...))
	}

	switch class {
	case ClassTransient, ClassThrottle:
		if c.aimd.onCongestion(class == ClassThrottle) {
			c.logLimitChange(ctx, "decrease", "endpoint congested, reducing S3 rate limit")
		}
	default:
		// A permanent error still means the endpoint answered, so it is not a
		// congestion signal; that failure is the syncer's problem, not the pacer's.
		if c.aimd.onHealthy() {
			c.logLimitChange(ctx, "increase", "endpoint recovering, raising S3 rate limit")
		}
	}
}

func (c *Client) logLimitChange(ctx context.Context, direction, msg string) {
	st := c.aimd.state()
	c.m.limitChanges.Add(ctx, 1, metric.WithAttributes(
		append(c.m.attrs, attribute.String("direction", direction))...))
	log.Warn().
		Float64("rate_limit", st.Current).
		Float64("base_rate_limit", st.Base).
		Bool("degraded", st.Degraded).
		Msg(msg)
}

// LimitState reports the endpoint's current pacing state.
func (c *Client) LimitState() LimitState { return c.aimd.state() }

func (c *Client) ListBuckets(ctx context.Context) ([]string, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	start := time.Now()
	out, err := c.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	c.recordOp(ctx, "ListBuckets", "", start, err)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		if b.Name != nil {
			names = append(names, *b.Name)
		}
	}
	return names, nil
}

// objectsFromContents converts one ListObjectsV2 page's Contents into Objects,
// skipping any entry with a nil Key (defensive; S3 does not document this as
// possible, but a nil dereference here would crash discovery). Shared by
// ListObjectsPage and the delimited listing used by ListObjectsTree.
func objectsFromContents(bucket string, contents []s3types.Object) []Object {
	objs := make([]Object, 0, len(contents))
	for _, item := range contents {
		if item.Key == nil {
			continue
		}
		obj := Object{
			Bucket: bucket,
			Key:    *item.Key,
			Size:   aws.ToInt64(item.Size),
		}
		if item.LastModified != nil {
			obj.ModifiedAt = *item.LastModified
		}
		if item.ETag != nil {
			obj.ETag = *item.ETag
		}
		objs = append(objs, obj)
	}
	return objs
}

// ListError wraps a failure from the underlying ListObjectsV2 call itself, as
// opposed to one returned by a caller's onPage callback (e.g. a state-write
// failure). Callers that need to tell these apart — discoverAndSyncBucket
// decides whether falling back to sharded discovery could plausibly help
// based on this — use errors.As rather than string-matching the message.
type ListError struct {
	Bucket string
	Err    error
}

func (e *ListError) Error() string { return fmt.Sprintf("list objects in %s: %v", e.Bucket, e.Err) }
func (e *ListError) Unwrap() error { return e.Err }

// ListObjectsPage fetches up to maxObjects from bucket starting after token,
// invoking onPage after each underlying S3 API page so callers can act on
// (e.g. transfer) objects as they are discovered instead of waiting for the
// full batch to accumulate. Returns the number of objects delivered, the
// continuation token for the next call (nil when the listing is exhausted),
// and any error — either from S3 or returned by onPage, which aborts the scan.
func (c *Client) ListObjectsPage(ctx context.Context, bucket, prefix string, token *string, maxObjects int, onPage func([]Object) error) (int, *string, error) {
	current := token
	var pageNum, collected int

	for collected < maxObjects {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: current,
		}
		if prefix != "" {
			input.Prefix = aws.String(prefix)
		}

		page, err := c.listPageWithRetry(ctx, input)
		if err != nil {
			return collected, nil, &ListError{Bucket: bucket, Err: err}
		}
		if page == nil {
			return collected, nil, &ListError{Bucket: bucket, Err: errNilListPage}
		}

		pageNum++
		objs := objectsFromContents(bucket, page.Contents)
		collected += len(objs)

		log.Debug().
			Str("bucket", bucket).
			Str("prefix", prefix).
			Int("page", pageNum).
			Int("page_objects", len(objs)).
			Int("total", collected).
			Msg("discovery page complete")

		if len(objs) > 0 {
			if err := onPage(objs); err != nil {
				return collected, nil, err
			}
		}

		if !aws.ToBool(page.IsTruncated) {
			return collected, nil, nil
		}
		current = page.NextContinuationToken

		if collected >= maxObjects {
			return collected, current, nil
		}
	}

	return collected, current, nil
}

const (
	// s3MaxAttempts overrides the SDK default of 3.
	s3MaxAttempts = 5

	listMaxRetries = 8
	// listMaxDelay caps the doubling so late attempts do not stall for minutes.
	listMaxDelay = 30 * time.Second
	// defaultListAttemptTimeout bounds a single ListObjectsV2 attempt, unless
	// Config.ListAttemptTimeout overrides it. Nothing else in this client
	// imposes a per-call deadline — a hanging server (observed: 2+ minutes with
	// zero response, no timeout, no error, from a MinIO bucket backend that
	// couldn't enumerate a large flat listing) would otherwise block the
	// caller's very first attempt forever, so the retry loop below never even
	// gets a chance to run. A slow-but-not-hanging backend under real
	// concurrent load (many discovery + transfer calls at once) may need more
	// than the default — see Config.ListAttemptTimeout.
	defaultListAttemptTimeout = 60 * time.Second
	// listAttemptTimeoutMaxFactor caps how far a timed-out attempt escalates
	// the next attempt's deadline, as a multiple of the base timeout. Past
	// this, a single page is so slow that the answer is a narrower prefix (or
	// a healthier backend), not more waiting.
	listAttemptTimeoutMaxFactor = 4
	// defaultListRetryBudget bounds the whole retry loop, unless
	// Config.ListRetryBudget overrides it. Escalating deadlines would
	// otherwise let an unanswerable prefix hold a discovery slot far longer
	// than the fixed-deadline loop this replaces; the budget keeps the
	// all-timeouts worst case at roughly what 8 fixed 60s attempts cost.
	defaultListRetryBudget = 10 * time.Minute
)

// listAttemptTimedOut reports whether a per-attempt sub-context — not the
// caller's outer ctx — is what expired. storage.Classify deliberately treats
// context.DeadlineExceeded as ClassOK ("cancellation is our own doing, never
// a congestion signal"), which is correct for the caller's own ctx but wrong
// here: a timeout listPageWithRetry imposes on itself is a transient failure
// that must be retried, not silently treated as an "OK" outcome.
func listAttemptTimedOut(outerCtx, attemptCtx context.Context, err error) bool {
	return outerCtx.Err() == nil && attemptCtx.Err() != nil && errors.Is(err, context.DeadlineExceeded)
}

// listErrClass classifies one list attempt for the congestion controller.
// A deadline the retry loop imposed on itself must count as congestion:
// Classify maps context.DeadlineExceeded to ClassOK ("cancellation is our own
// doing"), which is right for the caller's ctx but inverts the signal here.
// The backend failing to answer in time is exactly what AIMD exists to back
// off from, yet ClassOK feeds aimd.onHealthy(), which raises the rate limit
// and decays the failure score — so concurrent discovery workers timing out in
// a loop kept the endpoint pinned at "healthy", speeding up against a backend
// that was already too slow.
// escalateListTimeout returns the deadline for the next attempt. Retrying a
// deadline with the same deadline cannot succeed: if the backend needs 90s to
// answer, every 60s attempt fails identically and the loop merely burns
// listMaxRetries × timeout before giving up — observed in production as eight
// consecutive ~60s failures on the same prefix. Only a timeout escalates; a
// 504 needs another try, not a longer one.
func escalateListTimeout(cur, max time.Duration, timedOut bool) time.Duration {
	if !timedOut {
		return cur
	}
	return min(cur*2, max)
}

// unboundedRemaining stands in for "no budget" so callers can feed it straight
// into min(timeout, remaining) / min(delay, remaining) with no separate branch
// for the unbounded case — the same no-nil-pointer, no-branch convention this
// codebase already uses for rate.Inf ("the limiter is always constructed so
// the pointer is never nil").
const unboundedRemaining = time.Duration(math.MaxInt64)

// budgetRemaining reports how much of listRetryBudget is left for the next
// attempt, and whether the budget is exhausted. A non-positive listRetryBudget
// means unbounded: never exhausted, unboundedRemaining left. Without this
// gate, a negative budget (the "unbounded" convention shared with
// discoveryPrefixBudget) would make remaining negative from the very first
// attempt, breaking the loop immediately — the opposite of "unbounded".
func budgetRemaining(listRetryBudget time.Duration, began time.Time) (remaining time.Duration, exhausted bool) {
	if listRetryBudget <= 0 {
		return unboundedRemaining, false
	}
	remaining = listRetryBudget - time.Since(began)
	return remaining, remaining <= 0
}

func listErrClass(err error, timedOut bool) ErrClass {
	if timedOut {
		return ClassTransient
	}
	return Classify(err)
}

// errNilListPage guards the two call sites against a nil page: an empty result
// with no error crashed a pod in production (a limiter wait clobbered the retry
// loop's err). listPageWithRetry now enforces the invariant itself, so this is
// belt-and-braces — a panic here kills discovery, the transfer pool and the
// mgmt server's probes with it.
var errNilListPage = errors.New("list returned no page and no error")

// listPageWithRetry fetches a single ListObjectsV2 page, retrying transient
// errors (5xx, EOF, connection reset, broken pipe, or a per-attempt timeout)
// with exponential backoff.
func (c *Client) listPageWithRetry(ctx context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	bucket := aws.ToString(input.Bucket)
	prefix := aws.ToString(input.Prefix)
	timeout := c.listAttemptTimeout
	maxTimeout := c.listAttemptTimeout * listAttemptTimeoutMaxFactor
	began := time.Now()
	var err error
	for attempt := range listMaxRetries {
		// Scoped, not assigned to err: a successful wait would otherwise erase
		// the failure the loop is about to report (see the budget break below).
		if werr := c.wait(ctx); werr != nil {
			return nil, werr
		}
		// Escalating deadlines can outrun listMaxRetries, so the budget — not
		// the attempt count — is what bounds an unanswerable prefix.
		remaining, exhausted := budgetRemaining(c.listRetryBudget, began)
		if exhausted {
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, min(timeout, remaining))
		start := time.Now()
		var out *s3.ListObjectsV2Output
		out, err = c.s3.ListObjectsV2(attemptCtx, input)
		cancel()
		timedOut := listAttemptTimedOut(ctx, attemptCtx, err)
		c.recordOpClass(ctx, "ListObjectsV2", bucket, start, err, listErrClass(err, timedOut))
		if err == nil {
			return out, nil
		}
		if !isTransientErr(err) && !timedOut {
			return nil, err
		}
		timeout = escalateListTimeout(timeout, maxTimeout, timedOut)
		delay := min(time.Duration(1<<uint(attempt))*time.Second, listMaxDelay)
		// Jitter keeps replicas sharing an endpoint from retrying in lockstep.
		delay += rand.N(delay / 2)
		// Never sleep past the budget: the next iteration would only break.
		// A no-op when unbounded, since remaining is then unboundedRemaining.
		delay = min(delay, remaining)
		log.Warn().Err(err).Str("bucket", bucket).Str("prefix", prefix).
			Int("attempt", attempt+1).Str("retry_in", delay.String()).
			Str("next_timeout", timeout.String()).Bool("timed_out", timedOut).
			Msg("transient list error, retrying")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	// Invariant: a nil page is never returned with a nil error. Both exits
	// (retry count and budget) leave err holding the last attempt's failure;
	// synthesize one only if the loop somehow ran no attempt at all.
	if err == nil {
		err = fmt.Errorf("no attempt completed")
	}
	return nil, fmt.Errorf("list retries exhausted after %s: %w",
		time.Since(began).Round(time.Second), err)
}

// listDelimitedFn lists one page of a delimiter-scoped listing at prefix,
// resuming from token. Abstracts the real S3 call away from listObjectsTree's
// fan-out/fan-in orchestration, which has no S3 dependency and is unit-tested
// against a fake of this type.
type listDelimitedFn func(ctx context.Context, prefix string, token *string) (contents []Object, commonPrefixes []string, nextToken *string, err error)

// DiscoveryCheckpointer persists sharded discovery's resume point for one
// (bucket, prefix): the ListObjectsV2 continuation token of the page to fetch
// next. It exists because a prefix whose listing died partway through used to
// be re-listed from its first page on the next cycle — on a bucket whose
// individual pages are already at the limit of what the backend can answer,
// that restart is what makes the walk never terminate, however many cycles run.
//
// A checkpoint exists only while a prefix is INCOMPLETE. Completing a prefix
// clears it, so the next cycle lists that prefix in full again. That is
// deliberate: it is what gives an object whose *transfer* failed a chance to be
// rediscovered, and it costs little on a burn-after-reading bucket, which
// drains itself — a completed prefix is empty by the time it is re-listed.
//
// Declared here rather than in internal/state so internal/storage keeps no
// Redis dependency; *state.Store satisfies it structurally and is wired in
// cmd_sync.go (the same pattern as watcher.minioNotifier).
type DiscoveryCheckpointer interface {
	// LoadCheckpoint returns the stored token, or "" when there is none.
	LoadCheckpoint(ctx context.Context, bucket, prefix string) (token string, err error)
	SaveCheckpoint(ctx context.Context, bucket, prefix, token string) error
	ClearCheckpoint(ctx context.Context, bucket, prefix string) error
}

// prefixCheckpoint is listObjectsTree's bucket-bound view of a
// DiscoveryCheckpointer. It mirrors listDelimitedFn: the bucket is closed over,
// so the orchestration core stays bucket-agnostic and is faked in tests with an
// in-memory map rather than a Redis double. Unexported methods also keep the
// public seam at exactly DiscoveryCheckpointer.
type prefixCheckpoint interface {
	load(ctx context.Context, prefix string) (*string, error)
	save(ctx context.Context, prefix string, token *string) error
	clear(ctx context.Context, prefix string) error
}

type bucketCheckpoint struct {
	cp     DiscoveryCheckpointer
	bucket string
}

func (b bucketCheckpoint) load(ctx context.Context, prefix string) (*string, error) {
	tok, err := b.cp.LoadCheckpoint(ctx, b.bucket, prefix)
	if err != nil || tok == "" {
		return nil, err
	}
	return &tok, nil
}

func (b bucketCheckpoint) save(ctx context.Context, prefix string, token *string) error {
	return b.cp.SaveCheckpoint(ctx, b.bucket, prefix, aws.ToString(token))
}

func (b bucketCheckpoint) clear(ctx context.Context, prefix string) error {
	return b.cp.ClearCheckpoint(ctx, b.bucket, prefix)
}

// checkpointFor returns a bucket-bound checkpoint view, or a nil interface when
// checkpointing is disabled. Returning the typed zero value instead would yield
// a non-nil interface holding a nil implementation.
func (c *Client) checkpointFor(bucket string) prefixCheckpoint {
	if c.checkpoints == nil {
		return nil
	}
	return bucketCheckpoint{cp: c.checkpoints, bucket: bucket}
}

// ckptAction is what the single consumer goroutine must do about a page's
// checkpoint. The producer decides it (only it knows the prefix's pagination
// state); the consumer executes it (only it knows onPage accepted the page).
type ckptAction int

const (
	ckptNone  ckptAction = iota // checkpointing off, or this prefix opted out
	ckptSave                    // record token as this prefix's resume point
	ckptClear                   // prefix finished, or opted out: drop any resume point
)

// treePage is one unit of work for listObjectsTree's single consumer. The
// checkpoint bookkeeping travels with the objects so that both happen on that
// one goroutine, in page order — see the consumer loop for why the token must
// not be persisted by the producer.
type treePage struct {
	prefix string
	objs   []Object
	action ckptAction
	token  *string // resume point; set only when action == ckptSave
}

// listDelimitedPage returns a listDelimitedFn backed by a real "/"-delimited
// ListObjectsV2 call against bucket, going through the same retry (including
// the per-attempt timeout) as a flat listing.
func (c *Client) listDelimitedPage(bucket string) listDelimitedFn {
	return func(ctx context.Context, prefix string, token *string) ([]Object, []string, *string, error) {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: token,
		}
		page, err := c.listPageWithRetry(ctx, input)
		if err != nil {
			return nil, nil, nil, &ListError{Bucket: bucket, Err: fmt.Errorf("prefix %q: %w", prefix, err)}
		}
		if page == nil {
			return nil, nil, nil, &ListError{Bucket: bucket, Err: fmt.Errorf("prefix %q: %w", prefix, errNilListPage)}
		}
		objs := objectsFromContents(bucket, page.Contents)
		prefixes := make([]string, 0, len(page.CommonPrefixes))
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				prefixes = append(prefixes, *p.Prefix)
			}
		}
		var next *string
		if aws.ToBool(page.IsTruncated) {
			next = page.NextContinuationToken
		}
		// Debug, not Info: a bucket with a deep/wide key structure can visit
		// thousands of prefixes in one discovery cycle. This is what makes the
		// recursive descent (this prefix, its object count, and how many
		// subfolders it fans out into next) observable when diagnosing whether
		// sharded discovery is actually narrowing scope level by level, or stuck
		// re-listing the same prefix.
		log.Debug().
			Str("bucket", bucket).
			Str("prefix", prefix).
			Int("page_objects", len(objs)).
			Int("common_prefixes", len(prefixes)).
			Bool("truncated", next != nil).
			Msg("sharded discovery: prefix page complete")
		return objs, prefixes, next, nil
	}
}

// defaultShardedDiscoveryConcurrency bounds how many prefixes are actively
// being listed at once during a tree walk, unless
// Config.ShardedDiscoveryConcurrency overrides it — a struggling backend must
// not be hit with an unbounded burst of concurrent LIST calls just because
// the bucket happens to have many subfolders. Observed: a backend whose
// individual LIST latency is already several seconds even in isolation (e.g.
// a leaf-level folder with tens of thousands of objects) can push well past
// even a generous per-attempt timeout once several of these run concurrently
// alongside the transfer worker pool — see Config.ShardedDiscoveryConcurrency.
const defaultShardedDiscoveryConcurrency = 4

// defaultDiscoveryPrefixBudget bounds how long a single prefix may be listed in
// one sharded walk, unless Config.DiscoveryPrefixBudget overrides it.
// listRetryBudget bounds one *page*'s retry loop, so before this a prefix with
// many slow pages could hold a worker slot for hours: on a bucket with hundreds
// of prefixes, a handful of pathological ones monopolised every slot and the
// rest were never listed at all in that cycle. Yielding is cheap now that
// discovery checkpoints exist — the prefix resumes from the page it stopped on
// rather than restarting — so a bounded turn per prefix trades a slower finish
// for the walk actually reaching every prefix.
const defaultDiscoveryPrefixBudget = 10 * time.Minute

// ListObjectsTree recursively lists everything under rootPrefix using a
// "/"-delimited listing at each level — the same shape the MinIO/S3 web
// console uses to browse a bucket folder-by-folder — instead of one flat,
// bucket-wide listing that a backend struggling with a very large keyspace
// may never be able to answer (observed: a direct ListObjectsV2 call hanging
// 2+ minutes with zero response). Sibling prefixes are listed concurrently,
// bounded by c.shardedDiscoveryConcurrency.
func (c *Client) ListObjectsTree(ctx context.Context, bucket, rootPrefix string, onPage func([]Object) error) error {
	parked, err := listObjectsTree(ctx, bucket, rootPrefix, c.listDelimitedPage(bucket), onPage,
		c.shardedDiscoveryConcurrency, c.discoveryPrefixBudget, c.checkpointFor(bucket))
	c.recordParkedPrefixes(bucket, parked)
	return err
}

// recordParkedPrefixes stores bucket's parked-prefix count from the cycle that
// just finished (however it finished — partial counts on a fatal error or
// cancellation are still the best information available). ParkedPrefixes and
// the tranquila.s3.discovery.parked_prefixes gauge both read this.
func (c *Client) recordParkedPrefixes(bucket string, n int) {
	c.parkedMu.Lock()
	defer c.parkedMu.Unlock()
	if c.parkedPrefixes == nil {
		c.parkedPrefixes = make(map[string]int64)
	}
	c.parkedPrefixes[bucket] = int64(n)
}

// ParkedPrefixes reports how many sharded-discovery prefixes parked — resumed
// onto a checkpointed page the backend still could not answer, delivering
// zero pages — during bucket's most recently completed cycle. 0 means either
// none did, or the bucket has not been sharded-discovered on this Client yet.
func (c *Client) ParkedPrefixes(bucket string) int64 {
	c.parkedMu.Lock()
	defer c.parkedMu.Unlock()
	return c.parkedPrefixes[bucket]
}

// maxReportedPrefixErrs caps how many per-prefix failures are retained for the
// returned error. A bucket where every prefix is too slow to list would
// otherwise produce a joined error naming hundreds of them, logged in full on
// every cycle.
const maxReportedPrefixErrs = 10

// listObjectsTree is the S3-independent orchestration core of ListObjectsTree,
// unit-tested against a fake listDelimitedFn. onPage is invoked from a single
// goroutine only, one page at a time — never concurrently — so its existing
// contract (built in discoverAndSyncBucket, which mutates closed-over
// counters and a semaphore without locking) holds even though the listing
// calls that produce those pages run concurrently across many prefixes.
//
// A prefix whose listing fails is abandoned and reported, but does not stop
// the walk: on a very large bucket a handful of pathological prefixes must not
// discard every other prefix's progress (see docs/ARCHITECTURE.md).
//
// bucket is used for logging only — the listing itself gets it from the closure
// in list — but without it a per-prefix line is unattributable: several buckets
// are walked concurrently, so "prefix=20260206/" alone does not say whose.
func listObjectsTree(ctx context.Context, bucket, rootPrefix string, list listDelimitedFn, onPage func([]Object) error, concurrency int, prefixBudget time.Duration, ckpt prefixCheckpoint) (parked int, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tasks := make(chan string, 64)
	pages := make(chan treePage)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var fatalErr error
	var prefixErrs []error
	var prefixErrCount int
	var parkedCount int

	// setFatal aborts the whole walk: an onPage failure is the caller's (state
	// writes, cancellation), so continuing to list would be pointless.
	setFatal := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if fatalErr == nil {
			fatalErr = err
			cancel()
		}
	}
	getFatal := func() error {
		mu.Lock()
		defer mu.Unlock()
		return fatalErr
	}
	addPrefixErr := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		prefixErrCount++
		if len(prefixErrs) < maxReportedPrefixErrs {
			prefixErrs = append(prefixErrs, err)
		}
	}
	joinPrefixErrs := func() error {
		mu.Lock()
		defer mu.Unlock()
		if prefixErrCount > len(prefixErrs) {
			return errors.Join(append(prefixErrs,
				fmt.Errorf("and %d more prefixes failed", prefixErrCount-len(prefixErrs)))...)
		}
		return errors.Join(prefixErrs...)
	}
	// markParked records a prefix that resumed onto its checkpointed page and
	// still could not get past it — the one way checkpointing can stall,
	// bounded only by the checkpoint's TTL. Counted per walk, not tracked as
	// persistent state across walks: every cycle re-attempts every prefix from
	// the root, so a prefix the backend can answer again simply stops being
	// counted on its next cycle.
	markParked := func() {
		mu.Lock()
		defer mu.Unlock()
		parkedCount++
	}

	// enqueue reserves the WaitGroup slot synchronously (so a concurrent
	// wg.Wait() can never observe "done" before every discovered prefix is
	// accounted for) but performs the actual channel send on its own
	// goroutine, so a worker discovering many sibling prefixes never blocks
	// on tasks' capacity — only `concurrency` workers are ever blocked doing
	// the real (expensive) listing call.
	enqueue := func(prefix string) {
		wg.Add(1)
		go func() {
			select {
			case tasks <- prefix:
			case <-ctx.Done():
				wg.Done()
			}
		}()
	}

	// processOne fully paginates one prefix (which may span multiple pages),
	// enqueueing any subPrefixes it discovers along the way, and unconditionally
	// releases this task's WaitGroup slot via defer exactly once regardless of
	// which return path is taken — a plain `return` here, rather than `break`
	// inside the select below (which would only break the select, not this
	// loop, and double-release the slot on the next iteration).
	processOne := func(prefix string) {
		defer wg.Done()

		var token *string
		// dirty: this prefix may have a stored checkpoint, so a terminal event
		// must issue a clear. Set by a successful load, by every save, and also
		// by a FAILED load — a checkpoint we could not read may still be there,
		// and leaving it would strand sub-prefixes if this prefix turns out to
		// branch.
		var dirty, resumed bool
		// resumable: whether this prefix may be checkpointed at all. A delimited
		// listing returns objects and CommonPrefixes interleaved in one
		// paginated lexicographic stream, so resuming past page k would also
		// skip the sub-prefixes page k carried — stranding whole subtrees
		// silently. Checkpoint only while a prefix has produced no sub-prefixes:
		// that covers exactly the expensive case (a leaf folder spanning many
		// pages) and degrades to the un-checkpointed behaviour for the shallow
		// index levels that fan out.
		resumable := ckpt != nil

		if resumable {
			tok, err := ckpt.load(ctx, prefix)
			switch {
			case err != nil:
				// Never fail the prefix over this: the worst case is exactly the
				// pre-checkpointing behaviour, a full re-list.
				dirty = true
				log.Warn().Err(err).Str("bucket", bucket).Str("prefix", prefix).
					Msg("sharded discovery: checkpoint load failed, listing prefix from the start")
			case tok != nil:
				token, dirty, resumed = tok, true, true
				log.Info().Str("bucket", bucket).Str("prefix", prefix).
					Msg("sharded discovery: resuming prefix from stored checkpoint")
			}
		}

		// Bound the whole prefix, not each page. listRetryBudget only bounds one
		// page's retry loop, so a prefix with many slow pages could hold a
		// worker slot for hours while every other prefix waited. Cutting it off
		// is cheap once checkpointing is on: it resumes from the page it stopped
		// on next cycle rather than restarting. Derived from ctx, so cancelling
		// the walk still cancels the prefix.
		prefixCtx := ctx
		if prefixBudget > 0 {
			var cancelPrefix context.CancelFunc
			prefixCtx, cancelPrefix = context.WithTimeout(ctx, prefixBudget)
			defer cancelPrefix()
		}

		var pagesThisCycle int
		for {
			if ctx.Err() != nil {
				return
			}
			objs, subPrefixes, next, err := list(prefixCtx, prefix, token)
			if err != nil {
				// Abandon this prefix only, and leave its checkpoint intact —
				// that is the point: the next cycle restarts at the page that
				// failed rather than at the first one. Pages already delivered
				// stay synced; sub-prefixes not yet reached are found next cycle.
				if ctx.Err() == nil {
					// Running out of budget is a scheduling decision, not a
					// backend fault: the prefix yields its slot with its progress
					// banked, and the distinction matters when reading logs.
					budgetExhausted := prefixCtx.Err() != nil
					ev := log.Warn()
					switch {
					case budgetExhausted && pagesThisCycle > 0:
						ev = log.Info()
					case resumed && pagesThisCycle == 0:
						// The page this prefix is pinned to is still unanswerable,
						// so this cycle bought zero progress and the next one
						// starts here again. Loud, because a permanently
						// unanswerable page is the one way checkpointing can stall
						// (bounded only by the checkpoint's TTL).
						ev = log.Error()
						markParked()
					}
					ev.Err(err).Str("bucket", bucket).Str("prefix", prefix).
						Bool("resumed", resumed).Bool("budget_exhausted", budgetExhausted).
						Int("pages_this_cycle", pagesThisCycle).
						Msg("sharded discovery: prefix listing stopped early, continuing with other prefixes")
				}
				addPrefixErr(err)
				return
			}
			pagesThisCycle++

			action, tok := ckptNone, (*string)(nil)
			switch {
			case !resumable:
				// Already opted out; nothing to record.
			case len(subPrefixes) > 0:
				// Sub-prefixes make every later page unskippable. Opt out, and
				// clear anything an earlier cycle stored before it reached here.
				resumable = false
				if dirty {
					action, dirty = ckptClear, false
				}
			case next != nil:
				action, tok, dirty = ckptSave, next, true
			case dirty:
				// Prefix complete: drop the resume point so the NEXT cycle lists
				// it in full, which is what rediscovers objects whose transfer
				// failed.
				action, dirty = ckptClear, false
			}

			// A page carrying only CommonPrefixes has no objects but may still
			// carry a ckptClear, so it has to reach the consumer too. With
			// checkpointing off this collapses to the original len(objs) > 0.
			if len(objs) > 0 || action != ckptNone {
				select {
				case pages <- treePage{prefix: prefix, objs: objs, action: action, token: tok}:
				case <-ctx.Done():
					return
				}
			}
			for _, sp := range subPrefixes {
				enqueue(sp)
			}
			if next == nil {
				return
			}
			token = next
		}
	}

	worker := func() {
		for prefix := range tasks {
			processOne(prefix)
		}
	}

	for range concurrency {
		go worker()
	}
	enqueue(rootPrefix)

	go func() {
		wg.Wait()
		close(tasks)
		close(pages)
	}()

	// Single consumer. Beyond serializing onPage for its caller's benefit, this
	// is now load-bearing for checkpoint correctness: because `pages` is
	// unbuffered, a prefix's page k+1 cannot be sent until this loop has
	// finished page k's onPage AND page k's checkpoint write, making the
	// per-prefix ordering total with no extra synchronization.
	for p := range pages {
		if getFatal() != nil {
			continue // already aborting; drain so producers blocked on `pages <-` can exit
		}
		if len(p.objs) > 0 {
			if err := onPage(p.objs); err != nil {
				setFatal(err)
				// Deliberately skips the checkpoint write below: a rejected page
				// must never advance the resume point past objects nobody
				// accepted.
				continue
			}
		}
		// Only now, with this page's objects accepted, is it safe to record that
		// the walk may resume PAST them. Persisting from processOne instead
		// would race the handoff — the token could reach the store before, or
		// entirely without, the objects it skips ever reaching onPage.
		//
		// Checkpoint failures are never fatal: a store blip must not abort a
		// walk that is otherwise making progress. The cost is one lost resume
		// point, i.e. the pre-checkpointing behaviour for that prefix.
		switch p.action {
		case ckptSave:
			if err := ckpt.save(ctx, p.prefix, p.token); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("prefix", p.prefix).
					Msg("sharded discovery: checkpoint save failed, prefix restarts from the beginning next cycle")
			}
		case ckptClear:
			if err := ckpt.clear(ctx, p.prefix); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("prefix", p.prefix).
					Msg("sharded discovery: checkpoint clear failed, prefix may resume mid-way next cycle")
			}
		}
	}

	mu.Lock()
	parked = parkedCount
	mu.Unlock()

	if err := getFatal(); err != nil {
		return parked, err
	}
	if err := ctx.Err(); err != nil {
		return parked, err
	}
	return parked, joinPrefixErrs()
}

func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	c.recordOp(ctx, "HeadBucket", bucket, start, err)
	if err == nil {
		return nil
	}

	if err := c.wait(ctx); err != nil {
		return err
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if c.region != "" && c.region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(c.region),
		}
	}

	start = time.Now()
	_, err = c.s3.CreateBucket(ctx, input)
	c.recordOp(ctx, "CreateBucket", bucket, start, err)
	if err != nil {
		var alreadyExists *s3types.BucketAlreadyExists
		var alreadyOwned *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyExists) || errors.As(err, &alreadyOwned) {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", bucket, err)
	}
	return nil
}

// HeadObject returns the content length, CRC32 checksum, and ETag of an object.
// The CRC32 is populated only when the object was stored with a checksum algorithm;
// it is empty string otherwise. The ETag is always present: for a single-part
// upload it is the object's MD5 hex digest, comparable across independent
// uploads of identical content; for a multipart upload it is a composite of the
// parts' MD5s plus a "-partCount" suffix, comparable only to another upload
// that used the exact same part boundaries — see storage.SinglePartMD5.
func (c *Client) HeadObject(ctx context.Context, bucket, key string) (size int64, checksumCRC32, etag string, err error) {
	if err := c.wait(ctx); err != nil {
		return 0, "", "", err
	}
	start := time.Now()
	out, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	c.recordOp(ctx, "HeadObject", bucket, start, err)
	if err != nil {
		return 0, "", "", fmt.Errorf("head object %s/%s: %w", bucket, key, err)
	}
	return aws.ToInt64(out.ContentLength), aws.ToString(out.ChecksumCRC32), aws.ToString(out.ETag), nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, int64, error) {
	if err := c.wait(ctx); err != nil {
		return nil, 0, err
	}
	start := time.Now()
	out, err := c.tm.GetObject(ctx, &transfermanager.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	c.recordOp(ctx, "GetObject", bucket, start, err)
	if err != nil {
		return nil, 0, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	return io.NopCloser(out.Body), aws.ToInt64(out.ContentLength), nil
}

// PutObject uploads body to bucket/key using CRC32 checksum validation.
// Returns the base64-encoded CRC32 checksum from the upload response (empty if unavailable).
func (c *Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64) (checksumCRC32 string, err error) {
	if err := c.wait(ctx); err != nil {
		return "", err
	}
	start := time.Now()
	out, err := c.tm.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		Body:              body,
		ChecksumAlgorithm: tmtypes.ChecksumAlgorithmCrc32,
	})
	c.recordOp(ctx, "PutObject", bucket, start, err)
	if err != nil {
		return "", fmt.Errorf("put object %s/%s: %w", bucket, key, err)
	}
	return aws.ToString(out.ChecksumCRC32), nil
}

// DeleteObject removes an object from bucket. Returns nil if the object does not exist.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := c.wait(ctx); err != nil {
		return err
	}
	start := time.Now()
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	c.recordOp(ctx, "DeleteObject", bucket, start, err)
	if err != nil {
		return fmt.Errorf("delete object %s/%s: %w", bucket, key, err)
	}
	return nil
}
