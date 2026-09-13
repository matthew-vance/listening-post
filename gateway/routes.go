package main

import "net/http"

func addRoutes(mux *http.ServeMux) {
	mux.Handle("POST /v1/events", handleEventsPost())
}

func addAdminRoutes(mux *http.ServeMux, r *readiness) {
	mux.Handle("GET /healthz", handleHealthz())
	mux.Handle("GET /readyz", handleReadyz(r))
}
