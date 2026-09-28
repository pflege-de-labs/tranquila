package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
)

// faultProxy is an in-process L7 fault injector in front of an S3 endpoint.
//
// Toxiproxy operates on the TCP byte stream and has no HTTP parsing, so it can
// never emit a 504/503/500 response — it can only reset, stall or slice a
// connection. The production incident this suite exists to reproduce was an
// HTTP 504 GatewayTimeout from a gateway in front of MinIO, so status-code
// injection needs an L7 injector. Running it in-process keeps it precisely
// controllable from the test and adds no container.
type faultProxy struct {
	srv *httptest.Server

	mu        sync.Mutex
	remaining int  // requests still to fail; negative means "until cleared"
	status    int  // status to return while failing
	xmlBody   bool // emit an S3 XML error body, or a non-XML gateway page

	seen   atomic.Int64
	failed atomic.Int64
}

// newFaultProxy starts a proxy forwarding to upstream and registers cleanup.
func newFaultProxy(t *testing.T, upstream string) *faultProxy {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatalf("parse upstream %q: %v", upstream, err)
	}

	p := &faultProxy{}
	rp := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			// Host is deliberately left as the client sent it: SigV4 signs the
			// Host header, so rewriting it would invalidate every signature.
			// Silo (like MinIO) accepts a foreign Host under path-style addressing.
		},
	}

	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.seen.Add(1)
		if status, xmlBody, fail := p.next(); fail {
			p.failed.Add(1)
			p.writeFault(w, status, xmlBody)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

// next reports whether this request should fail, consuming one budgeted failure.
func (p *faultProxy) next() (status int, xmlBody, fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.remaining == 0 {
		return 0, false, false
	}
	if p.remaining > 0 {
		p.remaining--
	}
	return p.status, p.xmlBody, true
}

func (p *faultProxy) writeFault(w http.ResponseWriter, status int, xmlBody bool) {
	if xmlBody {
		// The shape MinIO/S3 returns, which the SDK decodes into an APIError.
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(status)
		code := map[int]string{
			http.StatusGatewayTimeout:      "GatewayTimeout",
			http.StatusBadGateway:          "BadGateway",
			http.StatusServiceUnavailable:  "ServiceUnavailable",
			http.StatusInternalServerError: "InternalError",
		}[status]
		if code == "" {
			code = "InternalError"
		}
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code>`+
			`<Message>%s</Message><Resource>/</Resource><RequestId>e2e</RequestId></Error>`,
			code, http.StatusText(status))
		return
	}
	// A gateway's own HTML error page: XML decoding fails and the SDK surfaces
	// no APIError at all, only the status. This is the harder path to classify.
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<html><head><title>%d %s</title></head><body><center><h1>%d %s</h1></center>"+
		"<hr><center>nginx</center></body></html>",
		status, http.StatusText(status), status, http.StatusText(status))
}

// URL is the endpoint to point a tranquila client at.
func (p *faultProxy) URL() string { return p.srv.URL }

// failNext makes the next n requests return status. xmlBody selects an S3 XML
// error body over a non-XML gateway page.
func (p *faultProxy) failNext(n, status int, xmlBody bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.remaining, p.status, p.xmlBody = n, status, xmlBody
}

// failAll makes every request return status until clear is called.
func (p *faultProxy) failAll(status int, xmlBody bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.remaining, p.status, p.xmlBody = -1, status, xmlBody
}

// clear stops injecting faults.
func (p *faultProxy) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.remaining = 0
}

func (p *faultProxy) stats() (seen, failed int64) {
	return p.seen.Load(), p.failed.Load()
}
