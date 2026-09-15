package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
)

type stationKey struct{}

// stationLookup is the port bearerAuth authenticates through; stationStore is the Postgres adapter.
type stationLookup interface {
	Lookup(ctx context.Context, token string) (station string, ok bool, err error)
}

// bearerAuth resolves the Authorization bearer token to a station and stores it in the request context.
func bearerAuth(logger *slog.Logger, stations stationLookup) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok {
				unauthorized(w)
				return
			}
			station, found, err := stations.Lookup(r.Context(), token)
			if err != nil {
				// A DB blip must not read as "your token is bad" to the client.
				logger.Error("lookup station", "err", err)
				fail(w, http.StatusInternalServerError, "internal")
				return
			}
			if !found {
				unauthorized(w)
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), stationKey{}, station)))
		})
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	fail(w, http.StatusUnauthorized, "unauthorized")
}

func stationFrom(ctx context.Context) string {
	station, _ := ctx.Value(stationKey{}).(string)
	return station
}
