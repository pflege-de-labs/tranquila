package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pflege-de-labs/tranquila/internal/state"
	internalsync "github.com/pflege-de-labs/tranquila/internal/sync"
	"github.com/rs/zerolog/log"
)

// Config holds the dependencies required by the management API server.
type Config struct {
	Addr     string
	State    *state.Store
	Progress *internalsync.Progress
	// Ready checks readiness dependencies. If nil, defaults to State.Ping.
	Ready func(ctx context.Context) error
	// Endpoints reports source and destination rate-limit state. Optional;
	// when nil the fields are omitted from the sync status response.
	Endpoints func() (source, destination EndpointState)
	// ParkedPrefixes reports how many sharded-discovery prefixes for bucket
	// parked (resumed onto a checkpointed page the backend still could not
	// answer) during its most recently completed cycle. Optional; when nil
	// the field is omitted from every bucket's response. Source-only in
	// practice, since nothing lists the destination.
	ParkedPrefixes func(bucket string) int64
}

// Server is the HTTP management API server.
type Server struct {
	srv            *http.Server
	state          *state.Store
	progress       *internalsync.Progress
	ready          func(ctx context.Context) error
	endpoints      func() (source, destination EndpointState)
	parkedPrefixes func(bucket string) int64
}

// NewServer creates a Server and registers all routes. Call ListenAndServe to start.
func NewServer(cfg Config) *Server {
	s := &Server{
		state:          cfg.State,
		progress:       cfg.Progress,
		ready:          cfg.Ready,
		endpoints:      cfg.Endpoints,
		parkedPrefixes: cfg.ParkedPrefixes,
	}
	if s.ready == nil && cfg.State != nil {
		s.ready = cfg.State.Ping
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /api/v1/buckets", s.requireDeps(s.listBuckets))
	mux.HandleFunc("GET /api/v1/buckets/{name}", s.requireDeps(s.getBucket))
	mux.HandleFunc("GET /api/v1/sync", s.requireDeps(s.getSyncStatus))
	s.srv = &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
		// Bound every phase of a request so a slow or idle client cannot hold a
		// connection open indefinitely. Generous enough for a probe or a curl.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	log.Info().Str("addr", s.srv.Addr).Msg("management API listening")
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ── JSON response types ──────────────────────────────────────────────────────

type BucketStats struct {
	Total   int64 `json:"total"`
	Synced  int64 `json:"synced"`
	Pending int64 `json:"pending"`
	Failed  int64 `json:"failed"`
}

type BucketSyncProgress struct {
	StartedAt      time.Time `json:"started_at"`
	PendingAtStart int64     `json:"pending_at_start"`
	SyncedThisRun  int64     `json:"synced_this_run"`
	FailedThisRun  int64     `json:"failed_this_run"`
	RatePerSec     float64   `json:"rate_per_sec"`
	ETASeconds     *float64  `json:"eta_seconds,omitempty"`
}

type BucketStatus struct {
	Name          string              `json:"name"`
	LastCollected *time.Time          `json:"last_collected,omitempty"`
	Stats         BucketStats         `json:"stats"`
	SyncProgress  *BucketSyncProgress `json:"sync_progress,omitempty"`
	// ParkedPrefixes is how many sharded-discovery prefixes resumed onto a
	// checkpointed page the backend still could not answer, as of this
	// bucket's most recently completed cycle. Omitted (not just zero) when
	// no source client is wired to report it, so "0" is only ever a real
	// observation, never "unknown".
	ParkedPrefixes *int64 `json:"parked_prefixes,omitempty"`
}

// EndpointState reports one endpoint's rate-limit pacing. RateLimit and
// BaseRateLimit are omitted when the endpoint is unlimited.
type EndpointState struct {
	RateLimit     float64    `json:"rate_limit,omitempty"`
	BaseRateLimit float64    `json:"base_rate_limit,omitempty"`
	Degraded      bool       `json:"degraded"`
	DegradedSince *time.Time `json:"degraded_since,omitempty"`
}

type SyncStatus struct {
	Running     bool           `json:"running"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	Source      *EndpointState `json:"source,omitempty"`
	Destination *EndpointState `json:"destination,omitempty"`
	Buckets     []BucketStatus `json:"buckets"`
}

type apiError struct {
	Error string `json:"error"`
}

// ── helpers ──────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) bucketStatus(ctx context.Context, name string, snap internalsync.ProgressSnapshot) (BucketStatus, error) {
	bs := BucketStatus{Name: name}

	ct, err := s.state.GetCollectionTime(ctx, name)
	if err != nil {
		return bs, err
	}
	if !ct.IsZero() {
		bs.LastCollected = &ct
	}

	st, err := s.state.BucketStats(ctx, name)
	if err != nil {
		return bs, err
	}
	bs.Stats = BucketStats{
		Total:   st.Total,
		Synced:  st.Synced,
		Pending: st.Pending,
		Failed:  st.Failed,
	}

	if s.parkedPrefixes != nil {
		n := s.parkedPrefixes(name)
		bs.ParkedPrefixes = &n
	}

	if bp, ok := snap.Buckets[name]; ok {
		elapsed := time.Since(bp.StartedAt).Seconds()
		var rate float64
		if elapsed > 0 && bp.Synced > 0 {
			rate = float64(bp.Synced) / elapsed
		}
		p := &BucketSyncProgress{
			StartedAt:      bp.StartedAt,
			PendingAtStart: bp.PendingAtStart,
			SyncedThisRun:  bp.Synced,
			FailedThisRun:  bp.Failed,
			RatePerSec:     rate,
		}
		if rate > 0 {
			remaining := float64(bp.PendingAtStart - bp.Synced - bp.Failed)
			if remaining > 0 {
				eta := remaining / rate
				p.ETASeconds = &eta
			}
		}
		bs.SyncProgress = p
	}

	return bs, nil
}

// requireDeps answers 503 instead of dereferencing a nil State/Progress; probe
// tests construct a Server without them on purpose.
func (s *Server) requireDeps(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.state == nil || s.progress == nil {
			writeJSON(w, http.StatusServiceUnavailable, apiError{Error: "state store not configured"})
			return
		}
		h(w, r)
	}
}

// ── handlers ─────────────────────────────────────────────────────────────────

// healthz is the liveness probe: returns 200 whenever the process is serving
// HTTP. It intentionally checks no dependencies — an unreachable Redis must not
// trigger a pod restart.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is the readiness probe: verifies the API's dependencies (Redis) are
// reachable so traffic is only routed to pods that can serve requests.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.ready != nil {
		if err := s.ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	names, err := s.state.ListBuckets(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}

	snap := s.progress.Snapshot()
	result := make([]BucketStatus, 0, len(names))
	for _, name := range names {
		bs, err := s.bucketStatus(ctx, name, snap)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		result = append(result, bs)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	ct, err := s.state.GetCollectionTime(ctx, name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	if ct.IsZero() {
		writeJSON(w, http.StatusNotFound, apiError{Error: "bucket not found: " + name})
		return
	}

	snap := s.progress.Snapshot()
	bs, err := s.bucketStatus(ctx, name, snap)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, bs)
}

func (s *Server) getSyncStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	snap := s.progress.Snapshot()

	status := SyncStatus{
		Running: snap.Running,
		Buckets: make([]BucketStatus, 0, len(snap.Buckets)),
	}
	if !snap.StartedAt.IsZero() {
		status.StartedAt = &snap.StartedAt
	}
	if s.endpoints != nil {
		src, dst := s.endpoints()
		status.Source, status.Destination = &src, &dst
	}

	for name := range snap.Buckets {
		bs, err := s.bucketStatus(ctx, name, snap)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		status.Buckets = append(status.Buckets, bs)
	}
	writeJSON(w, http.StatusOK, status)
}
