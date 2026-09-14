package main

import (
	"context"
	"net/http"
	"strings"
)

type stationKey struct{}

// bearerAuth resolves the Authorization bearer token to a station and stores it in the request context.
func bearerAuth(reg stationRegistry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			station, found := reg.lookup(token)
			if !ok || !found {
				w.Header().Set("WWW-Authenticate", "Bearer")
				encode(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), stationKey{}, station)))
		})
	}
}

func stationFrom(ctx context.Context) string {
	station, _ := ctx.Value(stationKey{}).(string)
	return station
}
