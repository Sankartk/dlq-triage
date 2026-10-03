package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/graph-gophers/graphql-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	maxBodyBytes = 1 << 20
	maxDepth     = 12
)

// Server wires the GraphQL API, health endpoints and metrics.
type Server struct {
	schema  *graphql.Schema
	auth    *Authenticator
	log     *slog.Logger
	metrics *Metrics
	ready   func(context.Context) error
	static  http.Handler
}

// Options configure NewServer.
type Options struct {
	Deps    Deps
	Auth    *Authenticator
	Metrics *Metrics
	// Ready is called by /readyz. A nil Ready means always ready.
	Ready func(context.Context) error
	// Static serves the dashboard for every path that is not an API route.
	Static http.Handler
}

// NewServer parses the schema against the resolvers and returns the server.
func NewServer(o Options) (*Server, error) {
	if o.Deps.Scans == nil {
		o.Deps.Scans = NewScanTracker()
	}
	s, err := graphql.ParseSchema(schema, newResolver(o.Deps),
		graphql.MaxDepth(maxDepth), graphql.UseFieldResolvers())
	if err != nil {
		return nil, err
	}
	log := o.Deps.Log
	if log == nil {
		log = slog.Default()
	}
	if o.Metrics == nil {
		o.Metrics = NewMetrics(prometheus.NewRegistry())
	}
	srv := &Server{schema: s, auth: o.Auth, log: log, metrics: o.Metrics, ready: o.Ready, static: o.Static}
	o.Auth.OnFailure = o.Metrics.AuthFailures.Inc
	return srv, nil
}

// Handler returns the full HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/graphql", s.auth.Middleware(http.HandlerFunc(s.graphql)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", s.readyz)
	// Metrics carry counts only (no payloads) and are meant for a scraper on a
	// private network; bind the service to a private interface or put it behind a proxy.
	mux.Handle("/metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	if s.static != nil {
		mux.Handle("/", s.static)
	}
	return securityHeaders(mux)
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type request struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, gqlError("use POST"))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, gqlError("request body is too large"))
			return
		}
		writeJSON(w, http.StatusBadRequest, gqlError("could not read the request body"))
		return
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil || req.Query == "" {
		writeJSON(w, http.StatusBadRequest, gqlError("request must be JSON with a query"))
		return
	}
	resp := s.schema.Exec(r.Context(), req.Query, req.OperationName, req.Variables)
	s.metrics.GraphQLRequests.WithLabelValues(outcome(resp)).Inc()
	s.metrics.GraphQLDuration.Observe(time.Since(start).Seconds())
	writeJSON(w, http.StatusOK, resp)
}

func outcome(r *graphql.Response) string {
	if len(r.Errors) > 0 {
		return "error"
	}
	return "ok"
}

func gqlError(msg string) map[string]any {
	return map[string]any{"errors": []map[string]string{{"message": msg}}}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
