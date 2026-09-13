package main

import "net/http"

func addRoutes(mux *http.ServeMux, r *readiness) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r))
}
