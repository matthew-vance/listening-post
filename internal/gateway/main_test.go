package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matthew-vance/listening-post/internal/kafkatest"
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

// The handler tables run on these func adapters, so every status code is reachable without Docker. The
// Postgres and Kafka adapters have their own tests (TestStationStoreLookup, TestHeartbeatStoreSave,
// TestKafkaPublisherPublish) and are wired end to end by TestRun.

const testStation = "3ae884ac-cac2-442d-93ec-5885d868f15c"

type lookupFunc func(ctx context.Context, token string) (string, bool, error)

func (f lookupFunc) Lookup(ctx context.Context, token string) (string, bool, error) {
	return f(ctx, token)
}

type saverFunc func(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error

func (f saverFunc) Save(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error {
	return f(ctx, station, receivedAt, hb)
}

type publisherFunc func(ctx context.Context, station string, receivedAt time.Time, events []event) error

func (f publisherFunc) Publish(ctx context.Context, station string, receivedAt time.Time, events []event) error {
	return f(ctx, station, receivedAt, events)
}

var (
	// fixedStations resolves testToken to testStation and nothing else.
	fixedStations = lookupFunc(func(_ context.Context, token string) (string, bool, error) {
		if token == testToken {
			return testStation, true, nil
		}
		return "", false, nil
	})
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
	failing := lookupFunc(func(context.Context, string) (string, bool, error) { return "", false, errors.New("boom") })

	tests := []struct {
		name, header string
		stations     stationLookup
		wantCode     int
		wantBody     string
	}{
		{"no header", "", fixedStations, http.StatusUnauthorized, ""},
		{"wrong scheme", "Basic dGVzdA==", fixedStations, http.StatusUnauthorized, ""},
		{"unknown token", "Bearer nope", fixedStations, http.StatusUnauthorized, ""},
		{"valid", "Bearer " + testToken, fixedStations, http.StatusOK, testStation},
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
		name     string
		ready    bool
		checks   map[string]func(context.Context) error
		wantCode int
		wantBody string
	}{
		{"not ready", false, allOK, http.StatusServiceUnavailable, "{\"status\":\"unavailable\"}\n"},
		{"ready", true, allOK, http.StatusOK, "{\"status\":\"ok\"}\n"},
		{"database down", true, dbDown, http.StatusServiceUnavailable, "{\"reason\":\"database\",\"status\":\"unavailable\"}\n"},
		{"kafka down", true, kafkaDown, http.StatusServiceUnavailable, "{\"reason\":\"kafka\",\"status\":\"unavailable\"}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &readiness{}
			r.ready.Store(tt.ready)

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
	var (
		published []event
		station   string
		received  time.Time
	)
	capture := publisherFunc(func(_ context.Context, s string, at time.Time, events []event) error {
		station, received, published = s, at, append(published, events...)
		return nil
	})
	srv := newServer(slog.New(slog.DiscardHandler), fixedStations, noopSaver, capture)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(srv, "/v1/events", tt.token, tt.body)
			wantStatus(t, rec, tt.wantCode)
			wantProblem(t, rec, tt.wantProblem)
		})
	}

	// the one 200 above published exactly its events, attributed to the authenticated station
	if station != testStation || received.IsZero() || len(published) != 1 || published[0].Raw != "MSG,3,1,1,ABC123,1" {
		t.Fatalf("published station=%q received=%v events=%+v", station, received, published)
	}

	t.Run("publish failure", func(t *testing.T) {
		failing := publisherFunc(func(context.Context, string, time.Time, []event) error { return errors.New("boom") })
		srv := newServer(slog.New(slog.DiscardHandler), fixedStations, noopSaver, failing)
		wantStatus(t, post(srv, "/v1/events", testToken, validEvents), http.StatusInternalServerError)
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
	var saved []heartbeatRequest
	capture := saverFunc(func(_ context.Context, station string, at time.Time, hb heartbeatRequest) error {
		if station != testStation || at.IsZero() {
			t.Errorf("save station=%q received=%v", station, at)
		}
		saved = append(saved, hb)
		return nil
	})
	srv := newServer(slog.New(slog.DiscardHandler), fixedStations, capture, noopPublisher)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(srv, "/v1/stations/heartbeat", tt.token, tt.body)
			wantStatus(t, rec, tt.wantCode)
			wantProblem(t, rec, tt.wantProblem)
		})
	}

	// the three 200s each reached the store; the optional fields arrive only when sent
	if len(saved) != 3 || saved[0].EventRate != nil || saved[1].EventRate == nil || *saved[1].EventRate != 6.4 || saved[2].OldestBufferedTS != nil {
		t.Fatalf("saved = %+v", saved)
	}

	t.Run("store failure", func(t *testing.T) {
		failing := saverFunc(func(context.Context, string, time.Time, heartbeatRequest) error { return errors.New("boom") })
		srv := newServer(slog.New(slog.DiscardHandler), fixedStations, failing, noopPublisher)
		wantStatus(t, post(srv, "/v1/stations/heartbeat", testToken, `{`+required+`}`), http.StatusInternalServerError)
	})
}

func TestRun(t *testing.T) {
	publicPort, adminPort := freePort(t), freePort(t)
	_, _, dbURL := testStations(t)
	topic := kafkatest.Topic(t)
	cfg := Config{
		Port:         publicPort,
		AdminPort:    adminPort,
		DatabaseURL:  dbURL,
		KafkaBrokers: kafkatest.Brokers,
		KafkaRaw:     topic,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, slog.New(slog.DiscardHandler)) }()

	if err := waitForReady(ctx, 2*time.Second, "http://localhost:"+adminPort+"/readyz"); err != nil {
		t.Fatal(err)
	}

	public := "http://localhost:" + publicPort
	if got := statusOf(t, http.MethodPost, public+"/v1/events", validEvents, testToken); got != http.StatusOK {
		t.Fatalf("POST /v1/events on public port: status = %d, want %d", got, http.StatusOK)
	}
	heartbeat := `{"reported_at":"2026-09-14T14:00:00Z","uptime_seconds":100,"disk_free_bytes":1000,"buffer_depth":0}`
	if got := statusOf(t, http.MethodPost, public+"/v1/stations/heartbeat", heartbeat, testToken); got != http.StatusOK {
		t.Fatalf("POST /v1/stations/heartbeat on public port: status = %d, want %d", got, http.StatusOK)
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
	cfg := Config{
		Port:         taken,
		AdminPort:    freePort(t),
		DatabaseURL:  testDB(t),
		KafkaBrokers: kafkatest.Brokers,
	}

	done := make(chan error, 1)
	go func() { done <- Run(t.Context(), cfg, slog.New(slog.DiscardHandler)) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run returned nil, want listen error")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("run did not return after listen failure")
	}
}

// post serves one authenticated JSON POST through h; an empty token sends no Authorization header.
func post(h http.Handler, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, want, rec.Body.String())
	}
}

// wantProblem asserts key is present in a 422 problems map; an empty key asserts nothing.
func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, key string) {
	t.Helper()
	if key == "" {
		return
	}
	var resp struct {
		Problems map[string]string `json:"problems"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if _, ok := resp.Problems[key]; !ok {
		t.Fatalf("problems = %v, want key %q", resp.Problems, key)
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
