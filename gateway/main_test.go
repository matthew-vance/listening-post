package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	validEvents = `{"events":[{"id":1,"ts":"2026-09-13T23:51:42.468150+00:00","raw":"MSG,3,1,1,ABC123,1"}]}`
	testToken   = "test-token"
)

func testRegistry() stationRegistry {
	return stationRegistry{hashToken(testToken): "dev"}
}

func TestLoadStations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stations.json")
	if err := os.WriteFile(path, []byte(`{"dev": "`+hashToken(testToken)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	reg, err := loadStations(path)
	if err != nil {
		t.Fatal(err)
	}
	if station, ok := reg.lookup(testToken); !ok || station != "dev" {
		t.Fatalf("lookup = %q, %v; want dev, true", station, ok)
	}
	if _, ok := reg.lookup("nope"); ok {
		t.Fatal("unknown token resolved to a station")
	}

	if _, err := loadStations(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing file: want error")
	}
}

func TestBearerAuth(t *testing.T) {
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(stationFrom(r.Context())))
	})
	h := bearerAuth(testRegistry())(echo)

	tests := []struct {
		name, header string
		wantCode     int
		wantBody     string
	}{
		{"no header", "", http.StatusUnauthorized, ""},
		{"wrong scheme", "Basic dGVzdA==", http.StatusUnauthorized, ""},
		{"unknown token", "Bearer nope", http.StatusUnauthorized, ""},
		{"valid", "Bearer " + testToken, http.StatusOK, "dev"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if tt.wantCode == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", rec.Header().Get("WWW-Authenticate"))
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	newAdminServer(&readiness{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
	if got, want := rec.Body.String(), "{\"status\":\"ok\"}\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name                string
		ready, shuttingDown bool
		wantCode            int
		wantBody            string
	}{
		{"not ready", false, false, http.StatusServiceUnavailable, "{\"status\":\"unavailable\"}\n"},
		{"ready", true, false, http.StatusOK, "{\"status\":\"ok\"}\n"},
		{"shutting down", true, true, http.StatusServiceUnavailable, "{\"status\":\"unavailable\"}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &readiness{}
			r.ready.Store(tt.ready)
			r.shuttingDown.Store(tt.shuttingDown)

			rec := httptest.NewRecorder()
			newAdminServer(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestEventsPost(t *testing.T) {
	tests := []struct {
		name        string
		token       string
		body        string
		wantCode    int
		wantProblem string // key expected in the 422 problems map
	}{
		{"valid", testToken, validEvents, http.StatusOK, ""},
		{"no auth", "", validEvents, http.StatusUnauthorized, ""},
		{"malformed json", testToken, `{"events":[`, http.StatusBadRequest, ""},
		{"bad ts", testToken, `{"events":[{"id":1,"ts":"nope","raw":"x"}]}`, http.StatusBadRequest, ""},
		{"empty events", testToken, `{"events":[]}`, http.StatusUnprocessableEntity, "events"},
		{"empty raw", testToken, `{"events":[{"id":1,"ts":"2026-09-13T23:51:42Z","raw":""}]}`, http.StatusUnprocessableEntity, "events[0].raw"},
		{"body too large", testToken, `{"events":[{"id":1,"ts":"2026-09-13T23:51:42Z","raw":"` + strings.Repeat("x", 1<<20) + `"}]}`, http.StatusRequestEntityTooLarge, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(tt.body))
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			newServer(slog.New(slog.DiscardHandler), testRegistry()).ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantProblem == "" {
				return
			}
			var resp struct {
				Problems map[string]string `json:"problems"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode body %q: %v", rec.Body.String(), err)
			}
			if _, ok := resp.Problems[tt.wantProblem]; !ok {
				t.Fatalf("problems = %v, want key %q", resp.Problems, tt.wantProblem)
			}
		})
	}
}

func TestHeartbeatPost(t *testing.T) {
	const required = `"reported_at":"2026-09-14T14:00:00Z","uptime_seconds":100,"disk_free_bytes":1000,"buffer_depth":0`
	tests := []struct {
		name        string
		token       string
		body        string
		wantCode    int
		wantProblem string
	}{
		{"required only", testToken, `{` + required + `}`, http.StatusOK, ""},
		{"all fields", testToken, `{` + required + `,"oldest_buffered_ts":"2026-09-14T13:59:50Z","last_publish_ts":"2026-09-14T13:59:58Z","last_event_ts":"2026-09-14T13:59:59Z","event_rate":6.4}`, http.StatusOK, ""},
		{"null optional", testToken, `{` + required + `,"oldest_buffered_ts":null}`, http.StatusOK, ""},
		{"no auth", "", `{` + required + `}`, http.StatusUnauthorized, ""},
		{"malformed json", testToken, `{`, http.StatusBadRequest, ""},
		{"bad reported_at", testToken, `{"reported_at":"nope","uptime_seconds":1,"disk_free_bytes":1,"buffer_depth":0}`, http.StatusBadRequest, ""},
		{"missing reported_at", testToken, `{"uptime_seconds":1,"disk_free_bytes":1,"buffer_depth":0}`, http.StatusUnprocessableEntity, "reported_at"},
		{"negative buffer_depth", testToken, `{"reported_at":"2026-09-14T14:00:00Z","uptime_seconds":1,"disk_free_bytes":1,"buffer_depth":-1}`, http.StatusUnprocessableEntity, "buffer_depth"},
		{"negative event_rate", testToken, `{` + required + `,"event_rate":-1}`, http.StatusUnprocessableEntity, "event_rate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/stations/heartbeat", strings.NewReader(tt.body))
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			newServer(slog.New(slog.DiscardHandler), testRegistry()).ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantProblem == "" {
				return
			}
			var resp struct {
				Problems map[string]string `json:"problems"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode body %q: %v", rec.Body.String(), err)
			}
			if _, ok := resp.Problems[tt.wantProblem]; !ok {
				t.Fatalf("problems = %v, want key %q", resp.Problems, tt.wantProblem)
			}
		})
	}
}

func TestRun(t *testing.T) {
	publicPort, adminPort := freePort(t), freePort(t)
	stations := writeStations(t)
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return publicPort
		case "ADMIN_PORT":
			return adminPort
		case "STATIONS_FILE":
			return stations
		}
		return ""
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- run(ctx, getenv, io.Discard) }()

	if err := waitForReady(ctx, 2*time.Second, "http://localhost:"+adminPort+"/readyz"); err != nil {
		t.Fatal(err)
	}

	public := "http://localhost:" + publicPort
	if got := statusOf(t, http.MethodPost, public+"/v1/events", validEvents, testToken); got != http.StatusOK {
		t.Fatalf("POST /v1/events on public port: status = %d, want %d", got, http.StatusOK)
	}
	if got := statusOf(t, http.MethodGet, public+"/healthz", "", ""); got != http.StatusNotFound {
		t.Fatalf("GET /healthz on public port: status = %d, want %d", got, http.StatusNotFound)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v, want nil", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

func TestRunReturnsWhenListenFails(t *testing.T) {
	// Wildcard bind: macOS lets ":port" coexist with "localhost:port" via SO_REUSEADDR.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	taken := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	stations := writeStations(t)
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return taken
		case "STATIONS_FILE":
			return stations
		}
		return freePort(t)
	}

	done := make(chan error, 1)
	go func() { done <- run(t.Context(), getenv, io.Discard) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run returned nil, want listen error")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("run did not return after listen failure")
	}
}

func writeStations(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stations.json")
	if err := os.WriteFile(path, []byte(`{"dev": "`+hashToken(testToken)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func statusOf(t *testing.T, method, url, body, token string) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

func waitForReady(ctx context.Context, timeout time.Duration, endpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", endpoint, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}
