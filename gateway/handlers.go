package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"time"
)

type event struct {
	ID  int64     `json:"id"`
	TS  time.Time `json:"ts"` // parse, don't validate: a bad ts fails decode
	Raw string    `json:"raw"`
}

type eventsRequest struct {
	Events []event `json:"events"`
}

func (r eventsRequest) Valid(_ context.Context) map[string]string {
	problems := map[string]string{}
	if len(r.Events) == 0 {
		problems["events"] = "must not be empty"
	}
	for i, e := range r.Events {
		if e.Raw == "" {
			problems[fmt.Sprintf("events[%d].raw", i)] = "must not be empty"
		}
	}
	return problems
}

// eventPublisher is the port handleEventsPost writes through; kafkaPublisher is the adapter.
type eventPublisher interface {
	Publish(ctx context.Context, station string, receivedAt time.Time, events []event) error
}

func handleEventsPost(logger *slog.Logger, pub eventPublisher) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		req, problems, err := decodeValid[eventsRequest](r)
		if respondDecodeError(w, problems, err) {
			return
		}
		station := stationFrom(r.Context())
		// publish.py gives up after 10s; answer well inside that so a slow broker reads as our 500, not its timeout
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := pub.Publish(ctx, station, time.Now(), req.Events); err != nil {
			logger.Error("publish events", "station", station, "count", len(req.Events), "err", err)
			encode(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		first, last := req.Events[0], req.Events[len(req.Events)-1]
		logger.Info("published events", "station", station, "count", len(req.Events), "first_id", first.ID, "last_id", last.ID)
		w.WriteHeader(http.StatusOK)
	})
}

type heartbeatRequest struct {
	ReportedAt    time.Time `json:"reported_at"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	DiskFreeBytes int64     `json:"disk_free_bytes"`
	BufferDepth   int64     `json:"buffer_depth"`
	// Optional diagnostics; pointers so absent/null is representable.
	OldestBufferedTS *time.Time `json:"oldest_buffered_ts"`
	LastPublishTS    *time.Time `json:"last_publish_ts"`
	LastEventTS      *time.Time `json:"last_event_ts"`
	EventRate        *float64   `json:"event_rate"`
}

func (r heartbeatRequest) Valid(_ context.Context) map[string]string {
	problems := map[string]string{}
	if r.ReportedAt.IsZero() {
		problems["reported_at"] = "must be set"
	}
	for name, v := range map[string]int64{
		"uptime_seconds":  r.UptimeSeconds,
		"disk_free_bytes": r.DiskFreeBytes,
		"buffer_depth":    r.BufferDepth,
	} {
		if v < 0 {
			problems[name] = "must not be negative"
		}
	}
	if r.EventRate != nil && *r.EventRate < 0 {
		problems["event_rate"] = "must not be negative"
	}
	return problems
}

// heartbeatSaver is the port handleHeartbeatPost writes through; heartbeatStore is the Postgres adapter.
type heartbeatSaver interface {
	Save(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error
}

func handleHeartbeatPost(logger *slog.Logger, store heartbeatSaver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
		req, problems, err := decodeValid[heartbeatRequest](r)
		if respondDecodeError(w, problems, err) {
			return
		}
		station := stationFrom(r.Context())
		if err := store.Save(r.Context(), station, time.Now(), req); err != nil {
			logger.Error("save heartbeat", "station", station, "err", err)
			encode(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
			return
		}
		attrs := []any{
			"station", station,
			"uptime_s", req.UptimeSeconds,
			"disk_free_bytes", req.DiskFreeBytes,
			"buffer_depth", req.BufferDepth,
			"clock_skew_s", time.Since(req.ReportedAt).Seconds(),
		}
		if req.OldestBufferedTS != nil {
			attrs = append(attrs, "oldest_buffered_ts", *req.OldestBufferedTS)
		}
		if req.LastPublishTS != nil {
			attrs = append(attrs, "last_publish_ts", *req.LastPublishTS)
		}
		if req.LastEventTS != nil {
			attrs = append(attrs, "last_event_ts", *req.LastEventTS)
		}
		if req.EventRate != nil {
			attrs = append(attrs, "event_rate", *req.EventRate)
		}
		logger.Info("heartbeat", attrs...)
		w.WriteHeader(http.StatusOK)
	})
}

// respondDecodeError writes the response for a failed decodeValid and reports whether it did.
func respondDecodeError(w http.ResponseWriter, problems map[string]string, err error) bool {
	var tooBig *http.MaxBytesError
	switch {
	case len(problems) > 0:
		encode(w, http.StatusUnprocessableEntity, map[string]any{"problems": problems})
	case errors.As(err, &tooBig):
		encode(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "body too large"})
	case err != nil:
		encode(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
	default:
		return false
	}
	return true
}

func handleHealthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// handleReadyz reports ready only when every named dependency check passes; the first failure names the reason.
func handleReadyz(r *readiness, checks map[string]func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !r.ready.Load() || r.shuttingDown.Load() {
			encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		for _, name := range slices.Sorted(maps.Keys(checks)) {
			if err := checks[name](ctx); err != nil {
				encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "reason": name})
				return
			}
		}
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// validator reports semantic problems with an already-decoded request, keyed by field.
type validator interface {
	Valid(ctx context.Context) (problems map[string]string)
}

func decodeValid[T validator](r *http.Request) (T, map[string]string, error) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, nil, fmt.Errorf("decode json: %w", err)
	}
	return v, v.Valid(r.Context()), nil
}

func encode[T any](w http.ResponseWriter, status int, v T) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // headers already sent; nothing useful to do with a write error
}
