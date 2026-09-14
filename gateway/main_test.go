package main

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	validEvents = `{"events":[{"id":1,"ts":"2026-09-13T23:51:42.468150+00:00","raw":"MSG,3,1,1,ABC123,1"}]}`
	testToken   = "test-token"
)

// testStations returns a station store on a fresh DB with one station registered under testToken,
// plus that station's id and the DB URL.
func testStations(t *testing.T) (*stationStore, string, string) {
	t.Helper()
	url := testDB(t)
	pool, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	id := insertStation(t, pool, testToken)
	return &stationStore{pool: pool}, id, url
}

// insertStation registers a station with one token and returns its generated id.
func insertStation(t *testing.T, pool *pgxpool.Pool, token string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(t.Context(), "INSERT INTO stations DEFAULT VALUES RETURNING id").Scan(&id); err != nil {
		t.Fatal(err)
	}
	insertToken(t, pool, id, token)
	return id
}

func insertToken(t *testing.T, pool *pgxpool.Pool, id, token string) {
	t.Helper()
	if _, err := pool.Exec(t.Context(), "INSERT INTO station_tokens (token_hash, station_id) VALUES ($1, $2)", hashToken(token), id); err != nil {
		t.Fatal(err)
	}
}

// lookupFunc adapts a func to stationLookup, for the one case real Postgres can't produce on demand: a failing lookup.
type lookupFunc func(ctx context.Context, token string) (string, bool, error)

func (f lookupFunc) Lookup(ctx context.Context, token string) (string, bool, error) {
	return f(ctx, token)
}

// saverFunc adapts a func to heartbeatSaver, for the one case real Postgres can't produce on demand: a failing save.
type saverFunc func(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error

func (f saverFunc) Save(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error {
	return f(ctx, station, receivedAt, hb)
}

// publisherFunc adapts a func to eventPublisher, for the one case a real broker can't produce on demand: a failing publish.
type publisherFunc func(ctx context.Context, station string, receivedAt time.Time, events []event) error

func (f publisherFunc) Publish(ctx context.Context, station string, receivedAt time.Time, events []event) error {
	return f(ctx, station, receivedAt, events)
}

var (
	noopSaver     = saverFunc(func(context.Context, string, time.Time, heartbeatRequest) error { return nil })
	noopPublisher = publisherFunc(func(context.Context, string, time.Time, []event) error { return nil })
	pingOK        = func(context.Context) error { return nil }
	allOK         = map[string]func(context.Context) error{"database": pingOK, "kafka": pingOK}
)

func TestStationStoreLookup(t *testing.T) {
	stations, id, _ := testStations(t)
	ctx := t.Context()
	resolves := func(token string) bool {
		t.Helper()
		station, ok, err := stations.Lookup(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		if ok && station != id {
			t.Fatalf("token resolved to %q, want %q", station, id)
		}
		return ok
	}

	if !resolves(testToken) || resolves("nope") {
		t.Fatal("initial: want testToken valid, nope invalid")
	}

	// rotation: add a second token, both work; revoke the first, only the second works
	insertToken(t, stations.pool, id, "second-token")
	if !resolves(testToken) || !resolves("second-token") {
		t.Fatal("after adding a token: want both valid")
	}
	if _, err := stations.pool.Exec(ctx, "UPDATE station_tokens SET revoked_at = now() WHERE token_hash = $1", hashToken(testToken)); err != nil {
		t.Fatal(err)
	}
	if resolves(testToken) || !resolves("second-token") {
		t.Fatal("after revoking first token: want only second valid")
	}

	// station kill switch
	if _, err := stations.pool.Exec(ctx, "UPDATE stations SET revoked_at = now() WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if resolves("second-token") {
		t.Fatal("after revoking station: want no token valid")
	}
}

func TestBearerAuth(t *testing.T) {
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(stationFrom(r.Context())))
	})
	stations, id, _ := testStations(t)
	failing := lookupFunc(func(context.Context, string) (string, bool, error) { return "", false, errors.New("boom") })

	tests := []struct {
		name, header string
		stations     stationLookup
		wantCode     int
		wantBody     string
	}{
		{"no header", "", stations, http.StatusUnauthorized, ""},
		{"wrong scheme", "Basic dGVzdA==", stations, http.StatusUnauthorized, ""},
		{"unknown token", "Bearer nope", stations, http.StatusUnauthorized, ""},
		{"valid", "Bearer " + testToken, stations, http.StatusOK, id},
		{"store error", "Bearer " + testToken, failing, http.StatusInternalServerError, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rec := httptest.NewRecorder()
			bearerAuth(slog.New(slog.DiscardHandler), tt.stations)(echo).ServeHTTP(rec, req)

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
	newAdminServer(&readiness{}, allOK).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

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
	pingFail := func(context.Context) error { return errors.New("connection refused") }
	dbDown := map[string]func(context.Context) error{"database": pingFail, "kafka": pingOK}
	kafkaDown := map[string]func(context.Context) error{"database": pingOK, "kafka": pingFail}
	tests := []struct {
		name                string
		ready, shuttingDown bool
		checks              map[string]func(context.Context) error
		wantCode            int
		wantBody            string
	}{
		{"not ready", false, false, allOK, http.StatusServiceUnavailable, "{\"status\":\"unavailable\"}\n"},
		{"ready", true, false, allOK, http.StatusOK, "{\"status\":\"ok\"}\n"},
		{"shutting down", true, true, allOK, http.StatusServiceUnavailable, "{\"status\":\"unavailable\"}\n"},
		{"database down", true, false, dbDown, http.StatusServiceUnavailable, "{\"reason\":\"database\",\"status\":\"unavailable\"}\n"},
		{"kafka down", true, false, kafkaDown, http.StatusServiceUnavailable, "{\"reason\":\"kafka\",\"status\":\"unavailable\"}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &readiness{}
			r.ready.Store(tt.ready)
			r.shuttingDown.Store(tt.shuttingDown)

			rec := httptest.NewRecorder()
			newAdminServer(r, tt.checks).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

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
	stations, id, _ := testStations(t)
	topic := testTopic(t)
	client, err := openKafka(t.Context(), kafkaBrokers)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pub := &kafkaPublisher{client: client, topic: topic}
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
			newServer(slog.New(slog.DiscardHandler), stations, noopSaver, pub).ServeHTTP(rec, req)

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

	// the one 200 above produced exactly one record, keyed by the station
	rec := consume(t, topic, 1)[0]
	if string(rec.Key) != id || !strings.Contains(string(rec.Value), `"raw":"MSG,3,1,1,ABC123,1"`) {
		t.Fatalf("record key=%q value=%s", rec.Key, rec.Value)
	}

	t.Run("publish failure", func(t *testing.T) {
		failing := publisherFunc(func(context.Context, string, time.Time, []event) error { return errors.New("boom") })
		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(validEvents))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		newServer(slog.New(slog.DiscardHandler), stations, noopSaver, failing).ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
	})
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
	stations, id, _ := testStations(t)
	pool := stations.pool
	store := &heartbeatStore{pool: pool}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/stations/heartbeat", strings.NewReader(tt.body))
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			rec := httptest.NewRecorder()
			newServer(slog.New(slog.DiscardHandler), stations, store, noopPublisher).ServeHTTP(rec, req)

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

	var stored int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM heartbeats WHERE station_id = $1", id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 1 { // three 200s share one reported_at, so one row
		t.Fatalf("stored heartbeats = %d, want 1", stored)
	}

	t.Run("store failure", func(t *testing.T) {
		failing := saverFunc(func(context.Context, string, time.Time, heartbeatRequest) error { return errors.New("boom") })
		req := httptest.NewRequest(http.MethodPost, "/v1/stations/heartbeat", strings.NewReader(`{`+required+`}`))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rec := httptest.NewRecorder()
		newServer(slog.New(slog.DiscardHandler), stations, failing, noopPublisher).ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
	})
}

func TestRun(t *testing.T) {
	publicPort, adminPort := freePort(t), freePort(t)
	_, _, dbURL := testStations(t)
	topic := testTopic(t)
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return publicPort
		case "ADMIN_PORT":
			return adminPort
		case "DATABASE_URL":
			return dbURL
		case "KAFKA_BROKERS":
			return strings.Join(kafkaBrokers, ",")
		case "KAFKA_TOPIC":
			return topic
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
	_, _, dbURL := testStations(t)
	getenv := func(key string) string {
		switch key {
		case "PORT":
			return taken
		case "DATABASE_URL":
			return dbURL
		case "KAFKA_BROKERS":
			return strings.Join(kafkaBrokers, ",")
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
