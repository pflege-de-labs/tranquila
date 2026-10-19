package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	s := NewServer(Config{Addr: ":0"})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	s.healthz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status field = %q, want %q", body["status"], "ok")
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name      string
		ready     func(ctx context.Context) error
		wantCode  int
		wantState string
		wantErr   bool
	}{
		{
			name:      "ready hook ok",
			ready:     func(ctx context.Context) error { return nil },
			wantCode:  http.StatusOK,
			wantState: "ok",
		},
		{
			name:      "ready hook error",
			ready:     func(ctx context.Context) error { return errors.New("redis down") },
			wantCode:  http.StatusServiceUnavailable,
			wantState: "unavailable",
			wantErr:   true,
		},
		{
			name:      "nil ready hook",
			ready:     nil,
			wantCode:  http.StatusOK,
			wantState: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewServer(Config{Addr: ":0", Ready: tt.ready})

			req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rec := httptest.NewRecorder()
			s.readyz(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["status"] != tt.wantState {
				t.Fatalf("status field = %q, want %q", body["status"], tt.wantState)
			}
			if tt.wantErr && body["error"] == "" {
				t.Fatalf("expected error field to be set")
			}
			if !tt.wantErr && body["error"] != "" {
				t.Fatalf("unexpected error field: %q", body["error"])
			}
		})
	}
}

func TestProbeRoutesRegistered(t *testing.T) {
	s := NewServer(Config{Addr: ":0", Ready: func(ctx context.Context) error { return nil }})
	srv := httptest.NewServer(s.srv.Handler)
	defer srv.Close()

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, http.StatusOK)
		}
	}
}

func TestDataRoutesWithoutStateReturn503(t *testing.T) {
	s := NewServer(Config{Addr: ":0"})
	srv := httptest.NewServer(s.srv.Handler)
	defer srv.Close()

	for _, path := range []string{"/api/v1/buckets", "/api/v1/buckets/foo", "/api/v1/sync"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, http.StatusServiceUnavailable)
		}
	}
}
