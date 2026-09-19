package history

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

// adminURL points at the shared Timescale container; each test gets its own database off it.
var adminURL string

func TestMain(m *testing.M) { kafkatest.Main(m, startTimescale) }

func startTimescale(ctx context.Context) (testcontainers.Container, error) {
	pg, err := tcpostgres.Run(ctx, "timescale/timescaledb:2.29.1-pg18",
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
