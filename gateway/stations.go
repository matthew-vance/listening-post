package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// stationRegistry maps hex(sha256(token)) to the station it authorizes.
type stationRegistry map[string]string

// loadStations reads {"<station>": "<sha256 hex>", ...} and inverts it for lookup by token.
func loadStations(path string) (stationRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read stations file: %w", err)
	}
	var byStation map[string]string
	if err := json.Unmarshal(data, &byStation); err != nil {
		return nil, fmt.Errorf("parse stations file %s: %w", path, err)
	}
	reg := make(stationRegistry, len(byStation))
	for station, hash := range byStation {
		if other, dup := reg[hash]; dup {
			return nil, fmt.Errorf("stations %q and %q share a token hash", other, station)
		}
		reg[hash] = station
	}
	return reg, nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// lookup keys by hash, so timing reveals nothing about the token itself.
func (s stationRegistry) lookup(token string) (station string, ok bool) {
	station, ok = s[hashToken(token)]
	return station, ok
}
