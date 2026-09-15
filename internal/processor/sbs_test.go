package processor

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Real dump1090 lines from the archive (type 2 is synthetic: an airborne receiver never emits it).
func TestParseSBS(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string // JSON with exactly the fields that should be present
	}{
		{"1 identification", "MSG,1,1,1,A4BF41,1,2026/09/14,16:05:25.403,2026/09/14,16:05:25.428,AAL433  ,,,,,,,,,,,0",
			`{"message_type":"MSG","transmission_type":1,"icao":"A4BF41","generated":"2026/09/14 16:05:25.403","logged":"2026/09/14 16:05:25.428","callsign":"AAL433","on_ground":false}`},
		{"2 surface position", "MSG,2,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,0,12.5,270.1,40.14684,-83.17065,,,,,,-1",
			`{"message_type":"MSG","transmission_type":2,"icao":"A22123","generated":"2026/09/14 16:05:24.167","logged":"2026/09/14 16:05:24.173","altitude":0,"ground_speed":12.5,"track":270.1,"lat":40.14684,"lon":-83.17065,"on_ground":true}`},
		{"3 airborne position", "MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0",
			`{"message_type":"MSG","transmission_type":3,"icao":"A22123","generated":"2026/09/14 16:05:24.167","logged":"2026/09/14 16:05:24.173","altitude":8275,"lat":40.14684,"lon":-83.17065,"alert":false,"spi":false,"on_ground":false}`},
		{"4 airborne velocity", "MSG,4,1,1,A51958,1,2026/09/14,15:35:33.455,2026/09/14,15:35:33.505,,,505,89,,,-64,,,,,0",
			`{"message_type":"MSG","transmission_type":4,"icao":"A51958","generated":"2026/09/14 15:35:33.455","logged":"2026/09/14 15:35:33.505","ground_speed":505,"track":89,"vertical_rate":-64,"on_ground":false}`},
		{"5 surveillance altitude", "MSG,5,1,1,A22123,1,2026/09/14,16:05:23.686,2026/09/14,16:05:23.737,,8275,,,,,,,0,,0,",
			`{"message_type":"MSG","transmission_type":5,"icao":"A22123","generated":"2026/09/14 16:05:23.686","logged":"2026/09/14 16:05:23.737","altitude":8275,"alert":false,"spi":false}`},
		{"6 surveillance id", "MSG,6,1,1,A86392,1,2026/09/14,16:06:05.266,2026/09/14,16:06:05.292,,,,,,,,6653,0,0,0,",
			`{"message_type":"MSG","transmission_type":6,"icao":"A86392","generated":"2026/09/14 16:06:05.266","logged":"2026/09/14 16:06:05.292","squawk":"6653","alert":false,"emergency":false,"spi":false}`},
		{"7 air to air", "MSG,7,1,1,A22123,1,2026/09/14,16:05:23.745,2026/09/14,16:05:23.791,,8275,,,,,,,,,,",
			`{"message_type":"MSG","transmission_type":7,"icao":"A22123","generated":"2026/09/14 16:05:23.745","logged":"2026/09/14 16:05:23.791","altitude":8275}`},
		{"8 all call reply", "MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,0",
			`{"message_type":"MSG","transmission_type":8,"icao":"AB197E","generated":"2026/09/14 16:05:23.670","logged":"2026/09/14 16:05:23.684","on_ground":false}`},
		{"squawk keeps leading zeros", "MSG,6,1,1,A86392,1,2026/09/14,16:06:05.266,2026/09/14,16:06:05.292,,,,,,,,0400,-1,-1,-1,",
			`{"message_type":"MSG","transmission_type":6,"icao":"A86392","generated":"2026/09/14 16:06:05.266","logged":"2026/09/14 16:06:05.292","squawk":"0400","alert":true,"emergency":true,"spi":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := parseSBS(tt.raw)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(msg)
			var g, w map[string]any
			json.Unmarshal(got, &g)
			json.Unmarshal([]byte(tt.want), &w)
			if !reflect.DeepEqual(g, w) {
				t.Fatalf("\n got %s\nwant %s", got, tt.want)
			}
		})
	}
}

func TestParseSBSErrors(t *testing.T) {
	tests := []struct{ name, raw string }{
		{"too few fields", "MSG,3,1,1,A22123"},
		{"empty", ""},
		{"not sbs", "hello from compose"},
		{"bad altitude", strings.Replace("MSG,3,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,8275,,,40.14684,-83.17065,,,0,,0,0", "8275", "high", 1)},
		{"bad transmission type", "MSG,x,1,1,A22123,1,2026/09/14,16:05:24.167,2026/09/14,16:05:24.173,,,,,,,,,,,,"},
		{"bad flag", "MSG,8,1,1,AB197E,1,2026/09/14,16:05:23.670,2026/09/14,16:05:23.684,,,,,,,,,,,,yes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseSBS(tt.raw); err == nil {
				t.Fatalf("parseSBS(%q): want error", tt.raw)
			}
		})
	}
}
