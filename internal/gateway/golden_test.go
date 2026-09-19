package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"
)

// The bodies a station sends are pinned by internal/wire/testdata (station/test_golden.py produces them); here
// the gateway must accept each one and hand the handler's adapter every field of it.

func loadGolden(t *testing.T, name string, into any) {
	t.Helper()
	data, err := os.ReadFile("../wire/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatal(err)
	}
}

// canonical re-decodes v as generic JSON with every RFC 3339 string normalised (Python writes +00:00, Go writes
// Z) and null members dropped: a station omits an optional field, the request struct marshals it as null.
func canonical(t *testing.T, v any) any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	var walk func(any) any
	walk = func(x any) any {
		switch x := x.(type) {
		case map[string]any:
			for k, v := range x {
				if v == nil {
					delete(x, k)
					continue
				}
				x[k] = walk(v)
			}
		case []any:
			for i, v := range x {
				x[i] = walk(v)
			}
		case string:
			if ts, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return ts.UTC().Format(time.RFC3339Nano)
			}
		}
		return x
	}
	return walk(out)
}

func TestHeartbeatGolden(t *testing.T) {
	var cases []struct {
		Name string          `json:"name"`
		Body json.RawMessage `json:"body"`
	}
	loadGolden(t, "heartbeat.json", &cases)
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			var saved *heartbeatRequest
			capture := saverFunc(func(_ context.Context, _ string, _ time.Time, hb heartbeatRequest) error {
				saved = &hb
				return nil
			})
			srv := newServer(slog.New(slog.DiscardHandler), fixedStations, capture, noopPublisher)
			wantStatus(t, post(srv, "/v1/stations/heartbeat", testToken, string(tc.Body)), http.StatusOK)
			var want any
			json.Unmarshal(tc.Body, &want)
			if got := canonical(t, saved); !reflect.DeepEqual(got, canonical(t, want)) {
				t.Fatalf("\n got %v\nwant %v", got, canonical(t, want))
			}
		})
	}
}

func TestEventsGolden(t *testing.T) {
	var golden struct {
		Body json.RawMessage `json:"body"`
	}
	loadGolden(t, "events.json", &golden)
	var published []event
	capture := publisherFunc(func(_ context.Context, _ string, _ time.Time, events []event) error {
		published = events
		return nil
	})
	srv := newServer(slog.New(slog.DiscardHandler), fixedStations, noopSaver, capture)
	wantStatus(t, post(srv, "/v1/events", testToken, string(golden.Body)), http.StatusOK)
	var want struct{ Events any }
	json.Unmarshal(golden.Body, &want)
	if got := canonical(t, published); !reflect.DeepEqual(got, canonical(t, want.Events)) {
		t.Fatalf("\n got %v\nwant %v", got, canonical(t, want.Events))
	}
}
