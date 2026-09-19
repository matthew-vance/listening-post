package processor

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The golden fixtures in internal/wire/testdata are the contract for events.decoded and aircraft.state; the
// Flink job's tests load the same files. Values compare as decoded JSON so number formatting doesn't matter.

func loadGolden[T any](t *testing.T, name string) []T {
	t.Helper()
	data, err := os.ReadFile("../wire/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var cases []T
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func asJSON(t *testing.T, v any) any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestDecodeGolden(t *testing.T) {
	type decodeCase struct {
		Name  string          `json:"name"`
		Value json.RawMessage `json:"value"` // an Event object, or a string for a value that isn't one
		Key   string          `json:"key"`
		Want  any             `json:"want"` // nil: must be rejected
	}
	for _, tc := range loadGolden[decodeCase](t, "decode.json") {
		t.Run(tc.Name, func(t *testing.T) {
			value := []byte(tc.Value)
			var s string
			if json.Unmarshal(tc.Value, &s) == nil {
				value = []byte(s)
			}
			out, err := decodeRecord("t", &kgo.Record{Value: value})
			if tc.Want == nil {
				if err == nil {
					t.Fatalf("want error, got %s", out.Value)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(out.Key) != tc.Key {
				t.Fatalf("key = %q, want %q", out.Key, tc.Key)
			}
			var got any
			json.Unmarshal(out.Value, &got)
			if !reflect.DeepEqual(got, tc.Want) {
				want, _ := json.Marshal(tc.Want)
				t.Fatalf("\n got %s\nwant %s", out.Value, want)
			}
		})
	}
}

func TestMergeGolden(t *testing.T) {
	type step struct {
		Station string    `json:"station"`
		TS      time.Time `json:"ts"`
		Raw     string    `json:"raw"`
		Emit    any       `json:"emit"` // the snapshot to publish after this step, or nil for none
	}
	type scenario struct {
		Name  string `json:"name"`
		Steps []step `json:"steps"`
	}
	for _, sc := range loadGolden[scenario](t, "merge.json") {
		t.Run(sc.Name, func(t *testing.T) {
			s := newState(time.Hour)
			for i, st := range sc.Steps {
				// one message per fold at a fixed clock: never a sweep, so the output is exactly the merge's verdict
				out := s.fold([]decodedIn{{Decoded: msg(st.Station, st.TS, st.Raw)}}, base)
				if st.Emit == nil {
					if len(out) != 0 {
						t.Fatalf("step %d: emitted %+v, want nothing", i, asJSON(t, out[0].snap))
					}
					continue
				}
				if len(out) != 1 {
					t.Fatalf("step %d: emitted %d records, want the snapshot %v", i, len(out), st.Emit)
				}
				if got := asJSON(t, out[0].snap); !reflect.DeepEqual(got, st.Emit) {
					g, _ := json.Marshal(got)
					w, _ := json.Marshal(st.Emit)
					t.Fatalf("step %d:\n got %s\nwant %s", i, g, w)
				}
			}
		})
	}
}
