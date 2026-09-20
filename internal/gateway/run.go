package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/matthew-vance/listening-post/internal/wire"
)

// Run starts the gateway and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error {
	pool, err := openDB(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("connected to database")

	kafka, err := wire.OpenKafka(ctx, cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer kafka.Close() // after the servers drain: flushes anything still in flight
	logger.Info("connected to kafka")

	pub := &kafkaPublisher{client: kafka, topic: cfg.KafkaRaw}
	checks := map[string]func(context.Context) error{"database": pool.Ping, "kafka": kafka.Ping}

	r := &readiness{}
	// ReadHeaderTimeout bounds how long a client can dribble headers before it costs us a goroutine.
	public := &http.Server{Addr: ":" + cfg.Port, Handler: newServer(logger, &stationStore{pool: pool}, &heartbeatStore{pool: pool}, pub), ReadHeaderTimeout: 10 * time.Second}
	admin := &http.Server{Addr: ":" + cfg.AdminPort, Handler: newAdminServer(r, logger, checks), ReadHeaderTimeout: 10 * time.Second}

	// Bind synchronously so a taken port fails run() outright and ready is only set once both listeners exist.
	errc := make(chan error, 2)
	for name, srv := range map[string]*http.Server{"public": public, "admin": admin} {
		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("%s: listen: %w", name, err)
		}
		logger.Info("listening", "server", name, "addr", srv.Addr)
		go func() {
			if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("%s: serve: %w", name, err)
			}
		}()
	}
	// Dependencies pinged, listeners bound: ready means what it says. Ongoing health is the checks in /readyz.
	r.ready.Store(true)

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errc: // a dead listener must not leave us running and reporting ready
	}
	r.ready.Store(false)
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Drain public first so /readyz reports 503 to probes for the whole drain window.
	return errors.Join(serveErr, public.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
}
