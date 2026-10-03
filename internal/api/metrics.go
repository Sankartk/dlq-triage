package api

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sankartk/dlq-triage/internal/domain"
)

// Metrics are the service's Prometheus metrics. They contain counts and
// durations only, never message content.
type Metrics struct {
	Registry        *prometheus.Registry
	GraphQLRequests *prometheus.CounterVec
	GraphQLDuration prometheus.Histogram
	AuthFailures    prometheus.Counter
	ScansTotal      *prometheus.CounterVec
	MessagesIngest  *prometheus.CounterVec
	ReplayJobs      *prometheus.CounterVec
	ReplayMessages  *prometheus.CounterVec
}

// NewMetrics registers the metrics on reg.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		Registry: reg,
		GraphQLRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlqtriage_graphql_requests_total", Help: "GraphQL requests by outcome."}, []string{"outcome"}),
		GraphQLDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "dlqtriage_graphql_request_seconds", Help: "GraphQL request duration.", Buckets: prometheus.DefBuckets}),
		AuthFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dlqtriage_auth_failures_total", Help: "Rejected API requests."}),
		ScansTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlqtriage_scans_total", Help: "Dead-letter queue scans by queue and result."}, []string{"queue", "result"}),
		MessagesIngest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlqtriage_messages_ingested_total", Help: "Messages stored for the first time."}, []string{"queue"}),
		ReplayJobs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlqtriage_replay_jobs_total", Help: "Finished replay jobs by queue and status."}, []string{"queue", "status"}),
		ReplayMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dlqtriage_replay_messages_total", Help: "Messages by replay outcome."}, []string{"queue", "outcome"}),
	}
	reg.MustRegister(m.GraphQLRequests, m.GraphQLDuration, m.AuthFailures, m.ScansTotal, m.MessagesIngest, m.ReplayJobs, m.ReplayMessages)
	return m
}

// ObserveScan records one scan.
func (m *Metrics) ObserveScan(queue string, seen, newMsgs int, err error) {
	if err != nil {
		m.ScansTotal.WithLabelValues(queue, "error").Inc()
		return
	}
	m.ScansTotal.WithLabelValues(queue, "ok").Inc()
	m.MessagesIngest.WithLabelValues(queue).Add(float64(newMsgs))
}

// ObserveJob records one finished real replay job.
func (m *Metrics) ObserveJob(j domain.ReplayJob) {
	m.ReplayJobs.WithLabelValues(j.Queue, string(j.Status)).Inc()
	m.ReplayMessages.WithLabelValues(j.Queue, "replayed").Add(float64(j.Replayed))
	m.ReplayMessages.WithLabelValues(j.Queue, "skipped").Add(float64(j.Skipped))
	m.ReplayMessages.WithLabelValues(j.Queue, "failed").Add(float64(j.Failed))
}
