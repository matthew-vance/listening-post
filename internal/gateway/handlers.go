package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/matthew-vance/listening-post/internal/pause"
)

type event struct {
	ID  int64     `json:"id"`
	TS  time.Time `json:"ts"` // parse, don't validate: a bad ts fails decode
	Raw string    `json:"raw"`
}

type eventsRequest struct {
	Events []event `json:"events"`
}

func (r eventsRequest) Valid() map[string]string {
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

type eventPublisher interface {
	Publish(ctx context.Context, station string, receivedAt time.Time, events []event) error
}

func handleEventsPost(logger *slog.Logger, pub eventPublisher, ingest *pause.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ingest.Paused() {
			fail(w, http.StatusServiceUnavailable, "ingest paused")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		req, ok := decodeValid[eventsRequest](w, r)
		if !ok {
			return
		}
		station := stationFrom(r.Context())
		if err := pub.Publish(r.Context(), station, time.Now(), req.Events); err != nil {
			logger.Error("publish events", "station", station, "count", len(req.Events), "err", err)
			fail(w, http.StatusInternalServerError, "internal")
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

func (r heartbeatRequest) Valid() map[string]string {
	problems := map[string]string{}
	if r.ReportedAt.IsZero() {
		problems["reported_at"] = "must be set"
	}
	if r.UptimeSeconds < 0 {
		problems["uptime_seconds"] = "must not be negative"
	}
	if r.DiskFreeBytes < 0 {
		problems["disk_free_bytes"] = "must not be negative"
	}
	if r.BufferDepth < 0 {
		problems["buffer_depth"] = "must not be negative"
	}
	if r.EventRate != nil && *r.EventRate < 0 {
		problems["event_rate"] = "must not be negative"
	}
	return problems
}

type heartbeatSaver interface {
	Save(ctx context.Context, station string, receivedAt time.Time, hb heartbeatRequest) error
}

func handleHeartbeatPost(logger *slog.Logger, store heartbeatSaver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 4<<10)
		req, ok := decodeValid[heartbeatRequest](w, r)
		if !ok {
			return
		}
		station := stationFrom(r.Context())
		if err := store.Save(r.Context(), station, time.Now(), req); err != nil {
			logger.Error("save heartbeat", "station", station, "err", err)
			fail(w, http.StatusInternalServerError, "internal")
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

func handleHealthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// handlePauseToggle pauses or resumes a named target (ingest or the archiver) and logs the action. When ingest
// is paused, POST /v1/events answers 503, so a station holds its batch and retries instead of publishing.
func handlePauseToggle(logger *slog.Logger, gate *pause.Gate, target string, paused bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if paused {
			gate.Pause()
			logger.Info("paused", "target", target)
		} else {
			gate.Resume()
			logger.Info("resumed", "target", target)
		}
		encode(w, http.StatusOK, map[string]bool{"paused": paused})
	})
}

// handleReadyz reports ready only when every named dependency check passes; the first failure names the reason.
func handleReadyz(r *readiness, checks map[string]func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !r.ready.Load() {
			encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		for name, check := range checks {
			if err := check(ctx); err != nil {
				encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "reason": name})
				return
			}
		}
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// validator reports semantic problems with an already-decoded request, keyed by field.
type validator interface {
	Valid() (problems map[string]string)
}

// decodeValid decodes and validates the body, writing the 400/413/422 itself; ok is false when it did.
func decodeValid[T validator](w http.ResponseWriter, r *http.Request) (v T, ok bool) {
	var tooBig *http.MaxBytesError
	err := json.NewDecoder(r.Body).Decode(&v)
	switch {
	case errors.As(err, &tooBig):
		fail(w, http.StatusRequestEntityTooLarge, "body too large")
	case err != nil:
		fail(w, http.StatusBadRequest, "invalid json")
	default:
		if problems := v.Valid(); len(problems) > 0 {
			encode(w, http.StatusUnprocessableEntity, map[string]any{"problems": problems})
			return v, false
		}
		return v, true
	}
	return v, false
}

func fail(w http.ResponseWriter, status int, msg string) {
	encode(w, status, map[string]string{"error": msg})
}

func encode(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // headers already sent; nothing useful to do with a write error
}
