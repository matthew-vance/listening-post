package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// sbsMessage is one SBS-1 BaseStation line (http://woodair.net/sbs/article/barebones42_socket_data.htm) as JSON.
// A field is present iff the line carried it; pointers + omitempty keep absent fields out of the document.
// Session, aircraft, and flight ids (fields 3, 4, 6) are dropped: dump1090 always emits 1.
type sbsMessage struct {
	MessageType      string `json:"message_type"`
	TransmissionType *int   `json:"transmission_type,omitempty"`
	ICAO             string `json:"icao,omitempty"`
	// Generated/logged are the receiver's clock in its local zone with no offset — kept verbatim, not parsed.
	// The envelope's ts (ingest clock, UTC) is the authoritative event time.
	Generated    string   `json:"generated,omitempty"`
	Logged       string   `json:"logged,omitempty"`
	Callsign     string   `json:"callsign,omitempty"`
	Altitude     *int     `json:"altitude,omitempty"`     // feet
	GroundSpeed  *float64 `json:"ground_speed,omitempty"` // knots
	Track        *float64 `json:"track,omitempty"`        // degrees
	Lat          *float64 `json:"lat,omitempty"`
	Lon          *float64 `json:"lon,omitempty"`
	VerticalRate *int     `json:"vertical_rate,omitempty"` // ft/min
	Squawk       string   `json:"squawk,omitempty"`        // string: leading zeros matter
	Alert        *bool    `json:"alert,omitempty"`
	Emergency    *bool    `json:"emergency,omitempty"`
	SPI          *bool    `json:"spi,omitempty"`
	OnGround     *bool    `json:"on_ground,omitempty"`
}

const sbsFields = 22

// parseSBS is positional and identical for every message type; the type only predicts which fields are filled.
func parseSBS(raw string) (sbsMessage, error) {
	f := strings.Split(raw, ",")
	if len(f) != sbsFields {
		return sbsMessage{}, fmt.Errorf("sbs: %d fields, want %d", len(f), sbsFields)
	}
	var (
		m   = sbsMessage{MessageType: f[0], ICAO: f[4], Callsign: strings.TrimSpace(f[10]), Squawk: f[17]}
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
		return sbsMessage{}, err
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
