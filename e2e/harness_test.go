package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pflege-de-labs/tranquila/internal/state"
	"github.com/pflege-de-labs/tranquila/internal/storage"
)

const (
	// Silo is a maintained MinIO fork (https://github.com/pgsty/silo). MinIO
	// ended community image distribution; quay.io/minio/minio now answers 401.
	// Pinned to a multi-arch RELEASE tag (amd64 + arm64), not latest.
	siloImage      = "docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z"
	toxiproxyImage = "ghcr.io/shopify/toxiproxy:2.12.0"
	redisImage     = "redis:7-alpine"

	s3User = "minioadmin"
	s3Pass = "minioadmin"

	// Port inside the toxiproxy container that fronts Silo.
	toxicS3Port = "8666"
)

// kvEngines are the Redis-compatible engines the state layer is verified
// against. It drives Lua scripts, so compatibility with forks is tested rather
// than assumed.
var kvEngines = []struct{ name, image string }{
	{"redis7", redisImage},
	{"valkey8", "valkey/valkey:8-alpine"},
	{"valkey9", "valkey/valkey:9-alpine"},
}

// newKV starts a single key-value container and returns a Store bound to it
// plus a raw client. Much lighter than newStack: the state layer needs neither
// S3 nor a proxy.
func newKV(t *testing.T, image string) (*state.Store, *redis.Client) {
	t.Helper()
	requireContainerRuntime(t)
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start %s: %v", image, err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })

	addr, err := c.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	st, err := state.NewStore(state.RedisConfig{Addr: addr})
	if err != nil {
		t.Fatalf("connect to %s: %v", image, err)
	}
	t.Cleanup(func() { _ = st.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	// Record what actually answered, so a passing run names the engine it proved.
	if info, err := rdb.InfoMap(ctx, "server").Result(); err == nil {
		if srv, ok := info["Server"]; ok {
			t.Logf("engine: %s (redis_version=%s valkey_version=%s)",
				image, srv["redis_version"], srv["valkey_version"])
		}
	}
	return st, rdb
}

// forEachEngine runs fn against every supported key-value engine.
func forEachEngine(t *testing.T, fn func(t *testing.T, st *state.Store, rdb *redis.Client)) {
	t.Helper()
	for _, e := range kvEngines {
		t.Run(e.name, func(t *testing.T) {
			st, rdb := newKV(t, e.image)
			fn(t, st, rdb)
		})
	}
}

// stack is the container fixture shared by a test: Silo for object storage,
// Redis for sync state, and Toxiproxy in front of Silo for L4 fault injection.
type stack struct {
	s3Endpoint    string // direct, no faults
	toxicEndpoint string // via toxiproxy
	redisAddr     string
	toxics        *toxiproxy.Proxy
}

// newStack brings up the container fixture and registers cleanup. Cleanup is
// explicit rather than relying on Ryuk, so the suite behaves the same whether
// or not the reaper is available on the local runtime.
func newStack(t *testing.T) *stack {
	requireContainerRuntime(t)
	t.Helper()
	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	// Silo keeps MinIO's MINIO_* variables and /minio/* routes unchanged.
	siloC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          siloImage,
			Cmd:            []string{"server", "/data"},
			Env:            map[string]string{"MINIO_ROOT_USER": s3User, "MINIO_ROOT_PASSWORD": s3Pass},
			ExposedPorts:   []string{"9000/tcp"},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {"silo"}},
			WaitingFor: wait.ForHTTP("/minio/health/live").
				WithPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start silo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(siloC) })

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        redisImage,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(redisC) })

	toxiC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        toxiproxyImage,
			ExposedPorts: []string{"8474/tcp", toxicS3Port + "/tcp"},
			Networks:     []string{net.Name},
			WaitingFor:   wait.ForListeningPort("8474/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start toxiproxy: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(toxiC) })

	s := &stack{}
	if s.s3Endpoint, err = siloC.PortEndpoint(ctx, "9000/tcp", "http"); err != nil {
		t.Fatalf("silo endpoint: %v", err)
	}
	redisEP, err := redisC.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("redis endpoint: %v", err)
	}
	s.redisAddr = redisEP

	ctrlEP, err := toxiC.PortEndpoint(ctx, "8474/tcp", "http")
	if err != nil {
		t.Fatalf("toxiproxy control endpoint: %v", err)
	}
	if s.toxicEndpoint, err = toxiC.PortEndpoint(ctx, toxicS3Port+"/tcp", "http"); err != nil {
		t.Fatalf("toxiproxy proxied endpoint: %v", err)
	}

	// Listen on all interfaces inside the container so the mapped port reaches it.
	tc := toxiproxy.NewClient(ctrlEP)
	s.toxics, err = tc.CreateProxy("silo", "0.0.0.0:"+toxicS3Port, "silo:9000")
	if err != nil {
		t.Fatalf("create toxiproxy proxy: %v", err)
	}
	t.Cleanup(func() { _ = s.toxics.Delete() })

	return s
}

// client builds a tranquila S3 client against endpoint. rateLimit of 0 leaves
// the endpoint unlimited, which disables congestion-driven degradation.
func (s *stack) client(t *testing.T, name, endpoint string, rateLimit float64, failThreshold int) *storage.Client {
	t.Helper()
	c, err := storage.NewClient(context.Background(), storage.Config{
		Endpoint:      endpoint,
		Region:        "us-east-1",
		AccessKey:     s3User,
		SecretKey:     s3Pass,
		RateLimit:     rateLimit,
		FailThreshold: failThreshold,
		Name:          name,
	})
	if err != nil {
		t.Fatalf("create %s client: %v", name, err)
	}
	return c
}

// store connects to the fixture's Redis.
func (s *stack) store(t *testing.T) *state.Store {
	t.Helper()
	st, err := state.NewStore(state.RedisConfig{Addr: s.redisAddr})
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// seed creates bucket and writes n objects into it, using a fault-free client.
func (s *stack) seed(t *testing.T, bucket string, n int) []string {
	t.Helper()
	ctx := context.Background()
	c := s.client(t, "seed", s.s3Endpoint, 0, 0)
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		t.Fatalf("ensure bucket %s: %v", bucket, err)
	}
	keys := make([]string, 0, n)
	for i := range n {
		key := fmt.Sprintf("obj-%03d.txt", i)
		body := strings.Repeat("x", 256)
		if _, err := c.PutObject(ctx, bucket, key, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("seed %s/%s: %v", bucket, key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

// countObjects reports how many of keys exist in bucket, via a fault-free client.
func (s *stack) countObjects(t *testing.T, bucket string, keys []string) int {
	t.Helper()
	ctx := context.Background()
	c := s.client(t, "verify", s.s3Endpoint, 0, 0)
	var n int
	for _, k := range keys {
		if _, _, _, err := c.HeadObject(ctx, bucket, k); err == nil {
			n++
		}
	}
	return n
}

// eventually polls cond until it holds or timeout elapses, so tests assert on
// convergence rather than on sleeps.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}
