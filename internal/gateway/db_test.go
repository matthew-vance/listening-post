package gateway

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/matthew-vance/listening-post/internal/kafkatest"
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// adminURL points at the shared test container; each test gets its own database off it.
var adminURL string

func TestMain(m *testing.M) { kafkatest.Main(m, startPostgres) }

func startPostgres(ctx context.Context) (testcontainers.Container, error) {
	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("postgres"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, err
	}
	adminURL, err = pg.ConnectionString(ctx, "sslmode=disable")
	return pg, err
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
	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	if _, err := migrator(t, u.String()).Up(ctx); err != nil {
		t.Fatal(err)
	}
	return u.String()
}

func migrator(t *testing.T, url string) *goose.Provider {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	p, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../db/migrations"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

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
	stations, id, _ := testStations(t)
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

	if err := store.Save(ctx, id, received, hb); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, id, received.Add(time.Second), hb); err != nil { // re-send: same (station, reported_at)
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
		FROM heartbeats WHERE station_id = $1`, id)
	if err := row.Scan(&count, &gotReceived, &gotLastEvt, &gotOldest, &gotRate, &gotDepth); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows = %d, want 1 (re-send must be ignored)", count)
	}

	if err := store.Save(ctx, "00000000-0000-0000-0000-000000000000", received, hb); err == nil {
		t.Fatal("save for unregistered station: want FK error")
	}
	if !gotReceived.Equal(received) || !gotLastEvt.Equal(reported) || gotOldest != nil || gotRate == nil || *gotRate != rate || gotDepth != 7 {
		t.Fatalf("row = received %v lastEvt %v oldest %v rate %v depth %d", gotReceived, gotLastEvt, gotOldest, gotRate, gotDepth)
	}
}
