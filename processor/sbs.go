package main

import (
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
	m.TransmissionType, err = optInt("transmission_type", f[1], err)
	m.Altitude, err = optInt("altitude", f[11], err)
	m.GroundSpeed, err = optFloat("ground_speed", f[12], err)
	m.Track, err = optFloat("track", f[13], err)
	m.Lat, err = optFloat("lat", f[14], err)
	m.Lon, err = optFloat("lon", f[15], err)
	m.VerticalRate, err = optInt("vertical_rate", f[16], err)
	m.Alert, err = optFlag("alert", f[18], err)
	m.Emergency, err = optFlag("emergency", f[19], err)
	m.SPI, err = optFlag("spi", f[20], err)
	m.OnGround, err = optFlag("on_ground", f[21], err)
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

// The opt* helpers thread one error through the field list so parseSBS reads as a table, not a ladder of ifs.

func optInt(name, s string, prev error) (*int, error) {
	if prev != nil || s == "" {
		return nil, prev
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("sbs: %s %q: not an integer", name, s)
	}
	return &v, nil
}

func optFloat(name, s string, prev error) (*float64, error) {
	if prev != nil || s == "" {
		return nil, prev
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("sbs: %s %q: not a number", name, s)
	}
	return &v, nil
}

// optFlag: SBS booleans are -1 (true) or 0 (false).
func optFlag(name, s string, prev error) (*bool, error) {
	if prev != nil || s == "" {
		return nil, prev
	}
	switch s {
	case "-1":
		v := true
		return &v, nil
	case "0":
		v := false
		return &v, nil
	}
	return nil, fmt.Errorf("sbs: %s %q: not -1 or 0", name, s)
}
