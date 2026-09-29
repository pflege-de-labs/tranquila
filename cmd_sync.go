package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pflege-de-labs/tranquila/config"
	"github.com/pflege-de-labs/tranquila/internal/api"
	"github.com/pflege-de-labs/tranquila/internal/state"
	"github.com/pflege-de-labs/tranquila/internal/storage"
	internalsync "github.com/pflege-de-labs/tranquila/internal/sync"
	"github.com/pflege-de-labs/tranquila/internal/telemetry"
	"github.com/pflege-de-labs/tranquila/internal/watcher"
	"github.com/rs/zerolog/log"
)

// S3Server holds connection and auth settings for one S3-compatible endpoint.
// It is embedded in SyncCmd twice — once for source, once for destination —
// with distinct flag and env-var prefixes applied by kong.
type S3Server struct {
	Endpoint  string  `name:"endpoint" env:"ENDPOINT" help:"S3-compatible endpoint URL (empty = AWS)"`
	Region    string  `name:"region" env:"REGION" default:"us-east-1" help:"AWS region"`
	AccessKey string  `name:"access-key" env:"ACCESS_KEY" help:"AWS access key ID"`
	SecretKey string  `name:"secret-key" env:"SECRET_KEY" help:"AWS secret access key"`
	RateLimit float64 `name:"rate-limit" env:"RATE_LIMIT" default:"0" help:"Max S3 API calls/sec (0 = unlimited)"`
}

type SyncCmd struct {
	Source      S3Server `embed:"" prefix:"source-" envprefix:"SOURCE_"`
	Destination S3Server `embed:"" prefix:"dest-" envprefix:"DEST_"`

	DestBucketPrefix string `name:"dest-bucket-prefix" env:"DEST_BUCKET_PREFIX" help:"Prefix prepended to auto-discovered destination bucket names"`

	BucketMappings    []string `name:"bucket-mappings" env:"BUCKET_MAPPINGS" help:"Bucket mappings: \"name\" or \"src=dst\". Comma-separated." sep:","`
	BucketMappingFile string   `name:"bucket-mapping-file" env:"BUCKET_MAPPING_FILE" help:"Path to file with bucket mappings (one per line)"`
	PrefixMappings    []string `name:"prefix-mappings" env:"PREFIX_MAPPINGS" help:"Path prefix mappings: \"bucket/src-prefix\" or \"bucket/src-prefix=dst-prefix\". Comma-separated." sep:","`

	RedisAddr     string `name:"redis-addr" env:"REDIS_ADDR" default:"localhost:6379" help:"Redis server address"`
	RedisPassword string `name:"redis-password" env:"REDIS_PASSWORD" help:"Redis password"`
	RedisDB       int    `name:"redis-db" env:"REDIS_DB" default:"0" help:"Redis database number"`
	RedisPoolSize int    `name:"redis-pool-size" env:"REDIS_POOL_SIZE" default:"0" help:"Redis connection pool size (0 = go-redis default: 10 * GOMAXPROCS)"`

	Workers             int  `name:"workers" env:"TRANQUILA_WORKERS" default:"10" help:"Number of concurrent sync workers"`
	CheckSizes          bool `name:"check-sizes" env:"TRANQUILA_CHECK_SIZES" default:"false" help:"Re-sync objects whose destination size differs from source"`
	DryRun              bool `name:"dry-run" env:"TRANQUILA_DRY_RUN" default:"false" help:"Log planned burn-after-reading deletions without executing them"`
	DiscoveryBatchSize  int  `name:"discovery-batch-size" env:"TRANQUILA_DISCOVERY_BATCH_SIZE" default:"100000" help:"Objects to discover per bucket before syncing; next batch starts after sync drains (0 = default 100000)"`
	MaxWorkersPerBucket int  `name:"max-workers-per-bucket" env:"TRANQUILA_MAX_WORKERS_PER_BUCKET" default:"0" help:"Cap on concurrent transfers for a single bucket, so one large bucket cannot starve others (0 = auto: half of --workers)"`

	ListAttemptTimeout          time.Duration `name:"list-attempt-timeout" env:"TRANQUILA_LIST_ATTEMPT_TIMEOUT" default:"0" help:"Starting timeout for a single ListObjectsV2 attempt, flat or sharded; doubles after each timed-out attempt up to 4x (0 = default 60s). See --list-retry-budget, which bounds how far escalation can actually reach."`
	ShardedDiscoveryConcurrency int           `name:"sharded-discovery-concurrency" env:"TRANQUILA_SHARDED_DISCOVERY_CONCURRENCY" default:"0" help:"Concurrent prefix listings during sharded discovery (0 = default 4); lower for a source backend whose LIST calls are slow even in isolation"`
	DiscoveryCheckpoints        bool          `name:"discovery-checkpoints" env:"TRANQUILA_DISCOVERY_CHECKPOINTS" default:"true" help:"Persist a per-prefix resume point during sharded discovery, so a prefix whose listing fails continues where it stopped on the next cycle instead of re-listing from its first page"`
	DiscoveryPrefixBudget       time.Duration `name:"discovery-prefix-budget" env:"TRANQUILA_DISCOVERY_PREFIX_BUDGET" default:"0" help:"How long one prefix may be listed in a single sharded walk before it yields its slot to other prefixes (0 = default 10m, negative = unbounded); see --list-retry-budget, a different scope (one page, not one prefix) that compounds with this"`
	DiscoveryCheckpointTTL      time.Duration `name:"discovery-checkpoint-ttl" env:"TRANQUILA_DISCOVERY_CHECKPOINT_TTL" default:"24h" help:"How long a sharded-discovery resume point stays valid without being refreshed; after this the prefix is listed from the start again"`
	ListRetryBudget             time.Duration `name:"list-retry-budget" env:"TRANQUILA_LIST_RETRY_BUDGET" default:"0" help:"How long listPageWithRetry may keep retrying one ListObjectsV2 page, including escalated per-attempt deadlines (0 = default 10m, negative = unbounded). See --discovery-prefix-budget, a different scope (one prefix's whole walk, not one page)."`

	Watch         bool          `name:"watch" env:"TRANQUILA_WATCH" default:"false" help:"Continuously re-run sync until interrupted"`
	WatchMode     string        `name:"watch-mode" env:"TRANQUILA_WATCH_MODE" default:"poll" enum:"poll,minio,sqs" help:"Watch backend: poll|minio|sqs"`
	WatchInterval time.Duration `name:"watch-interval" env:"TRANQUILA_WATCH_INTERVAL" default:"60s" help:"Idle time between poll cycles (poll mode only)"`
	SQSQueueURL   string        `name:"sqs-queue-url" env:"TRANQUILA_SQS_QUEUE_URL" help:"SQS queue URL for S3 event notifications (sqs mode)"`

	EndpointFailThreshold int           `name:"endpoint-fail-threshold" env:"TRANQUILA_ENDPOINT_FAIL_THRESHOLD" default:"5" help:"Consecutive transient endpoint failures before its rate limit is halved (requires --source/dest-rate-limit)"`
	CycleBackoff          time.Duration `name:"cycle-backoff" env:"TRANQUILA_CYCLE_BACKOFF" default:"5s" help:"Base delay before retrying a failed sync cycle in watch mode (exponential with jitter)"`
	CycleBackoffMax       time.Duration `name:"cycle-backoff-max" env:"TRANQUILA_CYCLE_BACKOFF_MAX" default:"10m" help:"Maximum delay between failed sync cycles in watch mode"`

	DeleteReconcileInterval time.Duration `name:"delete-reconcile-interval" env:"TRANQUILA_DELETE_RECONCILE_INTERVAL" default:"0s" help:"Cadence for detecting source deletions via a full Redis scan and propagating them to destinations with propagate-deletes enabled (0 = every cycle)"`

	BurnAfterReadingMinAge            config.Duration `name:"burn-after-reading-min-age" env:"TRANQUILA_BURN_AFTER_READING_MIN_AGE" default:"0s" help:"Global default: minimum source object age before burn-after-reading deletes it; supports d/w units (e.g. 7d, 2w) in addition to Go duration syntax. Overridable per bucket. 0 = delete immediately after verified sync (unchanged behavior)"`
	BurnAfterReadingReconcileInterval config.Duration `name:"burn-after-reading-reconcile-interval" env:"TRANQUILA_BURN_AFTER_READING_RECONCILE_INTERVAL" default:"30m" help:"Cadence for the independent background sweep that burns objects once they age past burn-after-reading-min-age; runs regardless of --watch-mode/--watch-interval, including in minio/sqs event-driven mode"`

	TelemetryExporter     string `name:"telemetry-exporter" env:"TELEMETRY_EXPORTER" default:"prometheus" enum:"prometheus,otlp,none" help:"Metrics exporter"`
	TelemetryAddr         string `name:"telemetry-addr" env:"TELEMETRY_ADDR" default:":8081" help:"Prometheus metrics listen address"`
	TelemetryOTLPEndpoint string `name:"telemetry-otlp-endpoint" env:"TELEMETRY_OTLP_ENDPOINT" help:"OTLP gRPC endpoint"`

	MgmtAddr string `name:"mgmt-addr" env:"MGMT_ADDR" default:":8080" help:"Management API listen address"`

	Buckets config.BucketMappings `name:"buckets" help:"Structured bucket mappings (from config file)"`
}

// endpointState adapts a storage rate-limit snapshot for the management API.
func endpointState(st storage.LimitState) api.EndpointState {
	es := api.EndpointState{
		RateLimit:     st.Current,
		BaseRateLimit: st.Base,
		Degraded:      st.Degraded,
	}
	if !st.Since.IsZero() {
		es.DegradedSince = &st.Since
	}
	return es
}

// Validate is called by kong after parsing, before Run. Workers sizes buffered
// channels, so a negative value panics the process and zero hangs it forever.
func (cmd *SyncCmd) Validate() error {
	if cmd.Workers < 1 {
		return fmt.Errorf("--workers must be at least 1, got %d", cmd.Workers)
	}
	return nil
}

// resolveBuckets merges --bucket-mappings, --prefix-mappings, and --bucket-mapping-file
// into a per-source-bucket config. The mapping file may contain both bucket-mapping lines
// ("name" or "src=dst") and prefix-mapping lines ("bucket/src-prefix[=dst-prefix]") —
// lines with a "/" in the source part are treated as prefix mappings.
// Returns nil when nothing is specified (signals the syncer to auto-discover).
func (cmd *SyncCmd) resolveBuckets() (map[string]internalsync.BucketConfig, error) {
	result := make(map[string]internalsync.BucketConfig)

	// Structured buckets from config file — processed first so CLI flags can override.
	for _, bm := range cmd.Buckets {
		src := bm.Source.Bucket
		if src == "" {
			continue
		}
		dst := bm.Destination.Bucket
		if dst == "" {
			dst = src
		}
		minAge := time.Duration(cmd.BurnAfterReadingMinAge)
		if bm.BurnAfterReadingMinAge != nil {
			minAge = time.Duration(*bm.BurnAfterReadingMinAge)
		}
		result[src] = internalsync.BucketConfig{
			Destination:            dst,
			SrcPrefix:              bm.Source.Prefix,
			DstPrefix:              bm.Destination.Prefix,
			BurnAfterReading:       bm.BurnAfterReading,
			BurnAfterReadingMinAge: minAge,
			PropagateDeletes:       bm.PropagateDeletes,
			ShardedDiscovery:       bm.ShardedDiscovery,
		}
	}

	// Legacy string-based mappings from CLI flags and mapping file.
	var entries []string
	entries = append(entries, cmd.BucketMappings...)
	entries = append(entries, cmd.PrefixMappings...)
	if cmd.BucketMappingFile != "" {
		fileEntries, err := loadMappingFile(cmd.BucketMappingFile)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fileEntries...)
	}

	var bucketEntries, prefixEntries []string
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		src, _, _ := strings.Cut(e, "=")
		if strings.ContainsRune(src, '/') {
			prefixEntries = append(prefixEntries, e)
		} else {
			bucketEntries = append(bucketEntries, e)
		}
	}

	for src, dst := range parseBucketMappings(bucketEntries) {
		result[src] = internalsync.BucketConfig{Destination: dst}
	}
	for _, e := range prefixEntries {
		src, dstPrefix, _ := strings.Cut(e, "=")
		bucket, srcPrefix, _ := strings.Cut(src, "/")
		bc := result[bucket]
		if bc.Destination == "" {
			bc.Destination = bucket
		}
		bc.SrcPrefix = srcPrefix
		bc.DstPrefix = dstPrefix
		result[bucket] = bc
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// parseBucketMappings converts "name" or "src=dst" strings into a map.
// For duplicate source keys, the last entry wins.
func parseBucketMappings(entries []string) map[string]string {
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" || strings.HasPrefix(e, "#") {
			continue
		}
		src, dst, found := strings.Cut(e, "=")
		if !found || dst == "" {
			dst = src
		}
		m[src] = dst
	}
	return m
}

// loadMappingFile reads one mapping per line from path (supports "name" and "src=dst", # comments).
func loadMappingFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bucket mapping file: %w", err)
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read bucket mapping file: %w", err)
	}
	return lines, nil
}

// warnIfListBudgetsBindFirst logs once when a bounded --list-retry-budget or
// --discovery-prefix-budget sits below the escalated --list-attempt-timeout
// ceiling, so it — not the attempt timeout — is what actually cuts a slow
// prefix off. By the time an operator sees a truncated deadline mid-run,
// they've already lost a cycle diagnosing the wrong knob; this turns it into
// a config lint at startup instead. Local consts mirror internal/storage's
// unexported defaults — exporting internal tuning constants across the
// package boundary for one warning is a bigger surface than the warning
// itself.
func warnIfListBudgetsBindFirst(cmd *SyncCmd) {
	const (
		defaultListAttemptTimeout    = 60 * time.Second // storage.defaultListAttemptTimeout
		listAttemptTimeoutMaxFactor  = 4                // storage.listAttemptTimeoutMaxFactor
		defaultListRetryBudget       = 10 * time.Minute // storage.defaultListRetryBudget
		defaultDiscoveryPrefixBudget = 10 * time.Minute // storage.defaultDiscoveryPrefixBudget
	)
	attemptTimeout := cmd.ListAttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = defaultListAttemptTimeout
	}
	ceiling := attemptTimeout * listAttemptTimeoutMaxFactor

	warnIfBelow := func(flag string, configured, def time.Duration) {
		b := configured
		if b == 0 {
			b = def
		}
		// A negative budget means unbounded, so it never binds first.
		if b > 0 && b < ceiling {
			log.Warn().Str("flag", flag).Dur("budget", b).Dur("escalation_ceiling", ceiling).
				Msg("this budget is lower than the escalated list-attempt-timeout ceiling and will bind first; raising --list-attempt-timeout alone will not avoid truncation")
		}
	}
	warnIfBelow("list-retry-budget", cmd.ListRetryBudget, defaultListRetryBudget)
	warnIfBelow("discovery-prefix-budget", cmd.DiscoveryPrefixBudget, defaultDiscoveryPrefixBudget)
}

func (cmd *SyncCmd) Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tel, err := telemetry.Setup(ctx, telemetry.Config{
		Exporter:     cmd.TelemetryExporter,
		Addr:         cmd.TelemetryAddr,
		OTLPEndpoint: cmd.TelemetryOTLPEndpoint,
	})
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer tel.Shutdown(context.Background())

	store, err := state.NewStore(state.RedisConfig{
		Addr:          cmd.RedisAddr,
		Password:      cmd.RedisPassword,
		DB:            cmd.RedisDB,
		PoolSize:      cmd.RedisPoolSize,
		CheckpointTTL: cmd.DiscoveryCheckpointTTL,
	})
	if err != nil {
		return fmt.Errorf("connect to redis: %w", err)
	}
	defer store.Close()

	// Left as a nil INTERFACE when disabled: assigning store unconditionally and
	// nil-checking the concrete type inside the client would instead produce a
	// non-nil interface holding a nil pointer, which it cannot detect.
	var checkpoints storage.DiscoveryCheckpointer
	if cmd.DiscoveryCheckpoints {
		checkpoints = store
	}

	warnIfListBudgetsBindFirst(cmd)

	log.Debug().Str("endpoint", cmd.Source.Endpoint).Str("region", cmd.Source.Region).Msg("creating source client")
	src, err := storage.NewClient(ctx, storage.Config{
		Endpoint:                    cmd.Source.Endpoint,
		Region:                      cmd.Source.Region,
		AccessKey:                   cmd.Source.AccessKey,
		SecretKey:                   cmd.Source.SecretKey,
		RateLimit:                   cmd.Source.RateLimit,
		FailThreshold:               cmd.EndpointFailThreshold,
		Name:                        "source",
		Meter:                       tel.Meter,
		ListAttemptTimeout:          cmd.ListAttemptTimeout,
		ShardedDiscoveryConcurrency: cmd.ShardedDiscoveryConcurrency,
		DiscoveryCheckpoints:        checkpoints,
		DiscoveryPrefixBudget:       cmd.DiscoveryPrefixBudget,
		ListRetryBudget:             cmd.ListRetryBudget,
	})
	if err != nil {
		return fmt.Errorf("create source S3 client: %w", err)
	}

	dst, err := storage.NewClient(ctx, storage.Config{
		Endpoint:      cmd.Destination.Endpoint,
		Region:        cmd.Destination.Region,
		AccessKey:     cmd.Destination.AccessKey,
		SecretKey:     cmd.Destination.SecretKey,
		RateLimit:     cmd.Destination.RateLimit,
		FailThreshold: cmd.EndpointFailThreshold,
		Name:          "destination",
		Meter:         tel.Meter,
	})
	if err != nil {
		return fmt.Errorf("create destination S3 client: %w", err)
	}

	bucketMap, err := cmd.resolveBuckets()
	if err != nil {
		return fmt.Errorf("resolve bucket mappings: %w", err)
	}

	progress := internalsync.NewProgress()

	mgmt := api.NewServer(api.Config{
		Addr:     cmd.MgmtAddr,
		State:    store,
		Progress: progress,
		Endpoints: func() (api.EndpointState, api.EndpointState) {
			return endpointState(src.LimitState()), endpointState(dst.LimitState())
		},
		ParkedPrefixes: src.ParkedPrefixes,
	})
	go func() {
		if err := mgmt.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("management API error")
		}
	}()
	defer mgmt.Shutdown(context.Background())

	syncer, err := internalsync.New(internalsync.Config{
		Source:                  src,
		Destination:             dst,
		State:                   store,
		Meter:                   tel.Meter,
		Buckets:                 bucketMap,
		DestBucketPrefix:        cmd.DestBucketPrefix,
		Workers:                 cmd.Workers,
		CheckSizes:              cmd.CheckSizes,
		DryRun:                  cmd.DryRun,
		Progress:                progress,
		DiscoveryBatchSize:      cmd.DiscoveryBatchSize,
		MaxWorkersPerBucket:     cmd.MaxWorkersPerBucket,
		CycleBackoff:            cmd.CycleBackoff,
		CycleBackoffMax:         cmd.CycleBackoffMax,
		DeleteReconcileInterval: cmd.DeleteReconcileInterval,
	})
	if err != nil {
		return fmt.Errorf("create syncer: %w", err)
	}

	log.Info().
		Int("workers", cmd.Workers).
		Float64("source_rate_limit", cmd.Source.RateLimit).
		Float64("dest_rate_limit", cmd.Destination.RateLimit).
		Bool("dry_run", cmd.DryRun).
		Bool("watch", cmd.Watch).
		Str("watch_mode", cmd.WatchMode).
		Str("telemetry", cmd.TelemetryExporter).
		Str("mgmt_addr", cmd.MgmtAddr).
		Msg("tranquila starting")

	var runErr error
	if !cmd.Watch {
		runErr = errors.Join(syncer.Run(ctx), syncer.ReconcileBurnAfterReading(ctx))
	} else {
		// The burn-after-reading min-age reconciler runs on its own schedule,
		// independent of --watch-mode/--watch-interval — see
		// Syncer.RunBurnAfterReadingReconciler. It must run concurrently with
		// whichever watch backend is selected, not as an alternative to it.
		var watchErr, reconcileErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			reconcileErr = syncer.RunBurnAfterReadingReconciler(ctx, time.Duration(cmd.BurnAfterReadingReconcileInterval))
		})
		wg.Go(func() {
			switch cmd.WatchMode {
			case "poll":
				watchErr = syncer.RunWatch(ctx, cmd.WatchInterval)
			case "minio":
				w, err := watcher.NewMinIO(watcher.MinIOConfig{
					Endpoint:  cmd.Source.Endpoint,
					AccessKey: cmd.Source.AccessKey,
					SecretKey: cmd.Source.SecretKey,
					Secure:    strings.HasPrefix(cmd.Source.Endpoint, "https://"),
				})
				if err != nil {
					watchErr = fmt.Errorf("create minio watcher: %w", err)
					return
				}
				watchErr = syncer.RunWatcher(ctx, w)
			case "sqs":
				w, err := watcher.NewSQS(watcher.SQSConfig{
					QueueURL:  cmd.SQSQueueURL,
					Region:    cmd.Source.Region,
					AccessKey: cmd.Source.AccessKey,
					SecretKey: cmd.Source.SecretKey,
				})
				if err != nil {
					watchErr = fmt.Errorf("create sqs watcher: %w", err)
					return
				}
				watchErr = syncer.RunWatcher(ctx, w)
			}
		})
		wg.Wait()
		runErr = errors.Join(watchErr, reconcileErr)
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}

	log.Info().Msg("tranquila done")
	return nil
}
