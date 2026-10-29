package engine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap/zaptest"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv, err := NewAtlasServer(zaptest.NewLogger(t), AtlasServerConfig{
		Engine: newTestAtlasConfig(t),
	})
	if err != nil {
		t.Fatalf("NewAtlasServer: %v", err)
	}
	return httptest.NewServer(srv.router)
}

func do(t *testing.T, ts *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func TestGivenPutThenGet_whenRequested_thenReturnsValue(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := do(t, ts, http.MethodPut, "/v1/atlas/foo", "bar")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT: got %d want 201", resp.StatusCode)
	}

	resp = do(t, ts, http.MethodGet, "/v1/atlas/foo", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: got %d want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "bar" {
		t.Errorf("GET body: got %q want %q", string(body), "bar")
	}
}

func TestGivenDeletedKey_whenGet_thenReturnsNoContent(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	do(t, ts, http.MethodPut, "/v1/atlas/foo", "bar")
	resp := do(t, ts, http.MethodDelete, "/v1/atlas/foo", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE: got %d want 200", resp.StatusCode)
	}

	resp = do(t, ts, http.MethodGet, "/v1/atlas/foo", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("GET after delete: got %d want 204", resp.StatusCode)
	}
}

func TestGivenUnknownKey_whenGet_thenReturnsNoContent(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := do(t, ts, http.MethodGet, "/v1/atlas/missing", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("got %d want 204", resp.StatusCode)
	}
}

func TestGivenHealthCheck_whenRequested_thenReturnsOk(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := do(t, ts, http.MethodGet, "/healthz", "")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("got %d want 200", resp.StatusCode)
	}
}

func TestGivenOversizedBody_whenPut_thenReturnsBadRequest(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// One byte above the cap should trip MaxBytesReader or validation.
	body := strings.Repeat("x", int(maxRequestBodyBytes)+1)
	resp := do(t, ts, http.MethodPut, "/v1/atlas/foo", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("got %d want 400", resp.StatusCode)
	}
}
