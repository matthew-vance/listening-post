package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	NewServer(&readiness{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

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
			NewServer(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantCode)
			}
			if rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestRun(t *testing.T) {
	port := freePort(t)
	getenv := func(key string) string {
		if key == "PORT" {
			return port
		}
		return ""
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	done := make(chan error, 1)
	go func() { done <- run(ctx, getenv, io.Discard) }()

	if err := waitForReady(ctx, 2*time.Second, "http://localhost:"+port+"/readyz"); err != nil {
		t.Fatal(err)
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
	start := time.Now()
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
			return ctx.Err()
		default:
			if time.Since(start) >= timeout {
				return fmt.Errorf("timeout waiting for %s", endpoint)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}
