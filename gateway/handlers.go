package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type event struct {
	ID  int64     `json:"id"`
	TS  time.Time `json:"ts"` // parse, don't validate: a bad ts fails decode
	Raw string    `json:"raw"`
}

type eventsRequest struct {
	Station string  `json:"station"`
	Events  []event `json:"events"`
}

func (r eventsRequest) Valid(_ context.Context) map[string]string {
	problems := map[string]string{}
	if r.Station == "" {
		problems["station"] = "must not be empty"
	}
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

func handleEventsPost() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		_, problems, err := decodeValid[eventsRequest](r)
		switch {
		case problems != nil:
			encode(w, http.StatusUnprocessableEntity, map[string]any{"problems": problems})
		case err != nil:
			encode(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
}

func handleHealthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		encode(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func handleReadyz(r *readiness) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if r.ready.Load() && !r.shuttingDown.Load() {
			encode(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		encode(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
	})
}

// Validator reports semantic problems with an already-decoded request, keyed by field.
type Validator interface {
	Valid(ctx context.Context) (problems map[string]string)
}

func decodeValid[T Validator](r *http.Request) (T, map[string]string, error) {
	var v T
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		return v, nil, fmt.Errorf("decode json: %w", err)
	}
	if problems := v.Valid(r.Context()); len(problems) > 0 {
		return v, problems, fmt.Errorf("invalid %T: %d problems", v, len(problems))
	}
	return v, nil, nil
}

func encode[T any](w http.ResponseWriter, status int, v T) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		return fmt.Errorf("encode json: %w", err)
	}
	return nil
}
