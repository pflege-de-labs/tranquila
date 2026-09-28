package e2e

import (
	"context"
	"net/http"
	"testing"

	"github.com/pflege-de-labs/tranquila/internal/storage"
)

// TestHarnessSmoke proves the fixture itself works before any behaviour is
// asserted on it: signed requests survive the in-process reverse proxy, the
// injector returns the statuses it is told to, real SDK errors classify as
// intended, and the L4 path via toxiproxy is reachable.
//
// Assertions use HeadObject rather than a listing: HeadObject has no
// repo-level retry wrapper, so a failing call costs only the SDK's own
// attempts (seconds) instead of listPageWithRetry's 8 jittered rounds
// (minutes). See e2e/README.md.
func TestHarnessSmoke(t *testing.T) {
	ctx := context.Background()
	s := newStack(t)
	keys := s.seed(t, "smoke-src", 3)

	t.Run("sigv4_survives_reverse_proxy", func(t *testing.T) {
		fp := newFaultProxy(t, s.s3Endpoint)
		c := s.client(t, "source", fp.URL(), 0, 0)
		count, _, err := c.ListObjectsPage(ctx, "smoke-src", "", nil, 100, func([]storage.Object) error { return nil })
		if err != nil {
			t.Fatalf("list through fault proxy: %v", err)
		}
		if count != len(keys) {
			t.Errorf("listed %d objects, want %d", count, len(keys))
		}
	})

	// Each case pins that a real error, produced by the real SDK against a real
	// HTTP status, lands in the class the resilience logic depends on.
	classCases := []struct {
		name    string
		status  int
		xmlBody bool
		want    storage.ErrClass
	}{
		{"504_xml_body", http.StatusGatewayTimeout, true, storage.ClassTransient},
		// A gateway HTML page: XML decoding fails so the SDK surfaces no
		// APIError at all, leaving only the status to classify on.
		{"504_non_xml_body", http.StatusGatewayTimeout, false, storage.ClassTransient},
		{"502_bad_gateway", http.StatusBadGateway, true, storage.ClassTransient},
		{"500_internal_error", http.StatusInternalServerError, true, storage.ClassTransient},
		{"503_is_throttle", http.StatusServiceUnavailable, true, storage.ClassThrottle},
		{"403_is_permanent", http.StatusForbidden, false, storage.ClassPermanent},
	}
	for _, tc := range classCases {
		t.Run("classify_"+tc.name, func(t *testing.T) {
			fp := newFaultProxy(t, s.s3Endpoint)
			c := s.client(t, "source", fp.URL(), 0, 0)
			fp.failAll(tc.status, tc.xmlBody)

			_, _, _, err := c.HeadObject(ctx, "smoke-src", keys[0])
			if err == nil {
				t.Fatalf("expected an error while every request returns %d", tc.status)
			}
			if got := storage.Classify(err); got != tc.want {
				t.Errorf("Classify(%d) = %v, want %v\nerr: %v", tc.status, got, tc.want, err)
			}
		})
	}

	t.Run("retry_absorbs_a_transient_burst", func(t *testing.T) {
		fp := newFaultProxy(t, s.s3Endpoint)
		c := s.client(t, "source", fp.URL(), 0, 0)
		// Fewer failures than the SDK's attempt budget, so the call must succeed.
		fp.failNext(2, http.StatusGatewayTimeout, true)

		if _, _, _, err := c.HeadObject(ctx, "smoke-src", keys[0]); err != nil {
			t.Errorf("HeadObject should have survived a 2-request 504 burst: %v", err)
		}
		if _, failed := fp.stats(); failed != 2 {
			t.Errorf("injector failed %d requests, want 2", failed)
		}
	})

	t.Run("toxiproxy_l4_path_is_reachable", func(t *testing.T) {
		c := s.client(t, "source", s.toxicEndpoint, 0, 0)
		if _, _, err := c.ListObjectsPage(ctx, "smoke-src", "", nil, 100, func([]storage.Object) error { return nil }); err != nil {
			t.Fatalf("list through toxiproxy: %v", err)
		}
	})
}
