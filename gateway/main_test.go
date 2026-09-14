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
	"strconv"
	"strings"
	"testing"
	"time"
)

const validEvents = `{"station":"dev","events":[{"id":1,"ts":"2026-09-13T23:51:42.468150+00:00","raw":"MSG,3,1,1,ABC123,1"}]}`

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
		body        string
		wantCode    int
		wantProblem string // key expected in the 422 problems map
	}{
		{"valid", validEvents, http.StatusOK, ""},
		{"malformed json", `{"station":"dev","events":[`, http.StatusBadRequest, ""},
		{"bad ts", `{"station":"dev","events":[{"id":1,"ts":"nope","raw":"x"}]}`, http.StatusBadRequest, ""},
		{"empty station", `{"station":"","events":[{"id":1,"ts":"2026-09-13T23:51:42Z","raw":"x"}]}`, http.StatusUnprocessableEntity, "station"},
		{"empty events", `{"station":"dev","events":[]}`, http.StatusUnprocessableEntity, "events"},
		{"empty raw", `{"station":"dev","events":[{"id":1,"ts":"2026-09-13T23:51:42Z","raw":""}]}`, http.StatusUnprocessableEntity, "events[0].raw"},
		{"body too large", `{"station":"dev","events":[{"id":1,"ts":"2026-09-13T23:51:42Z","raw":"` + strings.Repeat("x", 1<<20) + `"}]}`, http.StatusRequestEntityTooLarge, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			newServer(slog.New(slog.DiscardHandler)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(tt.body)))

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
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return publicPort
		case "ADMIN_PORT":
			return adminPort
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
	if got := statusOf(t, http.MethodPost, public+"/v1/events", validEvents); got != http.StatusOK {
		t.Fatalf("POST /v1/events on public port: status = %d, want %d", got, http.StatusOK)
	}
	if got := statusOf(t, http.MethodGet, public+"/healthz", ""); got != http.StatusNotFound {
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
	getenv := func(key string) string {
		if key == "PORT" {
			return taken
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

func statusOf(t *testing.T, method, url, body string) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
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
