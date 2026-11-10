package healthcheck

import (
	"context"
	"net/http"

	"go.temporal.io/sdk/client"
)

// Server answers /health and /ready with the Temporal connection's health.
type Server struct {
	temporal client.Client
}

func NewServer(c client.Client) *Server {
	return &Server{temporal: c}
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	if _, err := s.temporal.CheckHealth(context.Background(), &client.CheckHealthRequest{}); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) Run(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handler)
	mux.HandleFunc("/ready", s.handler)
	return http.ListenAndServe(addr, mux)
}
