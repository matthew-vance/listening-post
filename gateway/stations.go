package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// stationStore resolves bearer tokens against the station_tokens/stations tables.
type stationStore struct {
	pool *pgxpool.Pool
}

// Lookup returns the station a token authorizes. ok is false when the token is unknown, revoked, or belongs to a
// revoked station; err only for DB failure.
// ponytail: one query per authenticated request; cache by hash if it ever shows up in profiles.
func (s *stationStore) Lookup(ctx context.Context, token string) (station string, ok bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT s.id FROM station_tokens t JOIN stations s ON s.id = t.station_id
		WHERE t.token_hash = $1 AND t.revoked_at IS NULL AND s.revoked_at IS NULL`, hashToken(token),
	).Scan(&station)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("lookup station: %w", err)
	}
	return station, true, nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
