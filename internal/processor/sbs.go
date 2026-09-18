package processor

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/matthew-vance/listening-post/internal/wire"
)

const sbsFields = 22

// parseSBS is positional and identical for every message type; the type only predicts which fields are filled.
// It fills the SBS fields of a wire.Decoded; the envelope is the caller's.
func parseSBS(raw string) (wire.Decoded, error) {
	f := strings.Split(raw, ",")
	if len(f) != sbsFields {
		return wire.Decoded{}, fmt.Errorf("sbs: %d fields, want %d", len(f), sbsFields)
	}
	var (
		m   = wire.Decoded{MessageType: f[0], ICAO: f[4], Payload: wire.Payload{Callsign: strings.TrimSpace(f[10]), Squawk: f[17]}}
		err error
	)
	m.Generated = joinDateTime(f[6], f[7])
	m.Logged = joinDateTime(f[8], f[9])
	m.TransmissionType, err = opt("transmission_type", f[1], err, strconv.Atoi)
	m.Altitude, err = opt("altitude", f[11], err, strconv.Atoi)
	m.GroundSpeed, err = opt("ground_speed", f[12], err, parseFloat)
	m.Track, err = opt("track", f[13], err, parseFloat)
	m.Lat, err = opt("lat", f[14], err, parseFloat)
	m.Lon, err = opt("lon", f[15], err, parseFloat)
	m.VerticalRate, err = opt("vertical_rate", f[16], err, strconv.Atoi)
	m.Alert, err = opt("alert", f[18], err, parseFlag)
	m.Emergency, err = opt("emergency", f[19], err, parseFlag)
	m.SPI, err = opt("spi", f[20], err, parseFlag)
	m.OnGround, err = opt("on_ground", f[21], err, parseFlag)
	if err != nil {
		return wire.Decoded{}, err
	}
	return m, nil
}

func joinDateTime(date, clock string) string {
	if date == "" && clock == "" {
		return ""
	}
	return date + " " + clock
}

// opt parses one optional field, threading a prior error through so parseSBS reads as a table, not a ladder of ifs.
func opt[T any](name, s string, prev error, parse func(string) (T, error)) (*T, error) {
	if prev != nil || s == "" {
		return nil, prev
	}
	v, err := parse(s)
	if err != nil {
		return nil, fmt.Errorf("sbs: %s %q: %w", name, s, err)
	}
	return &v, nil
}

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

// parseFlag: SBS booleans are -1 (true) or 0 (false).
func parseFlag(s string) (bool, error) {
	switch s {
	case "-1":
		return true, nil
	case "0":
		return false, nil
	}
	return false, errors.New("not -1 or 0")
}
