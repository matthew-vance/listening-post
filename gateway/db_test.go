package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// adminURL points at the shared test container; each test gets its own database off it.
var adminURL string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		os.Exit(1)
	}
	adminURL, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(pg)
	os.Exit(code)
}

// testDB creates a fresh, migrated database for one test and returns its URL.
func testDB(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs docker")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	name := fmt.Sprintf("t_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("postgres://postgres:test@%s/%s?sslmode=disable", hostPort(t, adminURL), name)
	if _, err := migrator(t, url).Up(ctx); err != nil {
		t.Fatal(err)
	}
	return url
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), testDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func migrator(t *testing.T, url string) *goose.Provider {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../db/migrations"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// hostPort extracts "host:port" from a postgres URL.
func hostPort(t *testing.T, url string) string {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s:%d", cfg.ConnConfig.Host, cfg.ConnConfig.Port)
}

var _ = stdlib.GetDefaultDriver // registers the "pgx" database/sql driver

func TestMigrationsAreIdempotentAndReversible(t *testing.T) {
	url := testDB(t) // already at latest
	m := migrator(t, url)
	ctx := t.Context()

	again, err := m.Up(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("second Up applied %d migrations, want 0", len(again))
	}

	if _, err := m.DownTo(ctx, 0); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("Up after Down: %v", err)
	}
}

func TestHeartbeatStoreSave(t *testing.T) {
	stations, _ := testStations(t)
	pool := stations.pool
	store := &heartbeatStore{pool: pool}
	ctx := t.Context()

	reported := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
	received := reported.Add(2 * time.Second)
	rate := 6.4
	hb := heartbeatRequest{
		ReportedAt:    reported,
		UptimeSeconds: 100,
		DiskFreeBytes: 1000,
		BufferDepth:   7,
		LastEventTS:   &reported,
		EventRate:     &rate,
	}

	if err := store.Save(ctx, "dev", received, hb); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, "dev", received.Add(time.Second), hb); err != nil { // re-send: same (station, reported_at)
		t.Fatal(err)
	}

	var (
		count                   int
		gotReceived, gotLastEvt time.Time
		gotOldest               *time.Time
		gotRate                 *float64
		gotDepth                int64
	)
	row := pool.QueryRow(ctx, `SELECT count(*) OVER (), received_at, last_event_ts, oldest_buffered_ts, event_rate, buffer_depth
		FROM heartbeats WHERE station_id = 'dev'`)
	if err := row.Scan(&count, &gotReceived, &gotLastEvt, &gotOldest, &gotRate, &gotDepth); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1 (re-send must be ignored)", count)
	}

	if err := store.Save(ctx, "ghost", received, hb); err == nil {
		t.Fatal("save for unregistered station: want FK error")
	}

	// renaming a station carries its tokens and heartbeats with it
	if _, err := pool.Exec(ctx, "UPDATE stations SET id = 'renamed' WHERE id = 'dev'"); err != nil {
		t.Fatal(err)
	}
	var moved int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM heartbeats WHERE station_id = 'renamed')
		+ (SELECT count(*) FROM station_tokens WHERE station_id = 'renamed')`).Scan(&moved); err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("rows following the rename = %d, want 2 (1 heartbeat + 1 token)", moved)
	}
	if !gotReceived.Equal(received) || !gotLastEvt.Equal(reported) || gotOldest != nil || gotRate == nil || *gotRate != rate || gotDepth != 7 {
		t.Fatalf("row = received %v lastEvt %v oldest %v rate %v depth %d", gotReceived, gotLastEvt, gotOldest, gotRate, gotDepth)
	}
}
