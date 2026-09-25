package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

type Metrics struct {
	Registry *prometheus.Registry

	transactions *prometheus.CounterVec
	duplicates   *prometheus.CounterVec
	retries      *prometheus.CounterVec
	dlq          *prometheus.CounterVec
	conflicts    *prometheus.CounterVec
	processing   *prometheus.HistogramVec
	lockWait     prometheus.Histogram
	reconDiverge prometheus.Counter
	outboxLag    prometheus.Gauge
	outboxPub    *prometheus.CounterVec
	outboxFail   prometheus.Counter
	sqsMessages  *prometheus.CounterVec
	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
}

var _ application.Metrics = (*Metrics)(nil)

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_transactions_total",
			Help: "Wager transactions by final or waiting status, per source, kind and failure code.",
		}, []string{"source", "kind", "status", "failure_code"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_duplicates_total",
			Help: "Repeated operations: outcome=replay (same content) or conflict (different content).",
		}, []string{"source", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_retries_total",
			Help: "Retries by component (sqs_consumer, reference_worker, outbox_publisher).",
		}, []string{"component"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_dlq_messages_total",
			Help: "Messages sent to the dead-letter queue by reason.",
		}, []string{"reason"}),
		conflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_concurrency_conflicts_total",
			Help: "Concurrency conflicts: deadlock, serialization, lock_timeout, duplicate_insert.",
		}, []string{"reason"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wagering_processing_duration_seconds",
			Help:    "End-to-end processing latency of an operation, per source.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"source"}),
		lockWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wagering_wallet_lock_wait_seconds",
			Help:    "Time spent acquiring the per-wallet row lock.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}),
		reconDiverge: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_reconciliation_divergences_total",
			Help: "Reconciliations whose stored balance differs from the ledger.",
		}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wagering_outbox_lag_seconds",
			Help: "Age of the oldest unpublished outbox event.",
		}),
		outboxPub: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_outbox_published_total",
			Help: "Outbox events published, by event type.",
		}, []string{"event_type"}),
		outboxFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_outbox_publish_failures_total",
			Help: "Failed publication attempts of outbox events.",
		}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_sqs_messages_total",
			Help: "Input messages by outcome: processed, duplicate, retry, dead_lettered, released.",
		}, []string{"outcome"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_http_requests_total",
			Help: "HTTP requests by route pattern and status code.",
		}, []string{"route", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wagering_http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		}, []string{"route"}),
	}
	m.Registry.MustRegister(
		m.transactions, m.duplicates, m.retries, m.dlq, m.conflicts, m.processing, m.lockWait,
		m.reconDiverge, m.outboxLag, m.outboxPub, m.outboxFail, m.sqsMessages, m.httpRequests, m.httpDuration,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

func (m *Metrics) TransactionResult(source application.Source, kind domain.TransactionKind, status domain.TransactionStatus, code domain.FailureCode) {
	m.transactions.WithLabelValues(string(source), string(kind), string(status), string(code)).Inc()
}

func (m *Metrics) Duplicate(source application.Source, outcome string) {
	m.duplicates.WithLabelValues(string(source), outcome).Inc()
}

func (m *Metrics) ProcessingDuration(source application.Source, d time.Duration) {
	m.processing.WithLabelValues(string(source)).Observe(d.Seconds())
}

func (m *Metrics) ConcurrencyConflict(reason string) { m.conflicts.WithLabelValues(reason).Inc() }
func (m *Metrics) WalletLockWait(d time.Duration)    { m.lockWait.Observe(d.Seconds()) }
func (m *Metrics) ReconciliationDivergence()         { m.reconDiverge.Inc() }
func (m *Metrics) Retry(component string)            { m.retries.WithLabelValues(component).Inc() }

func (m *Metrics) DeadLetter(reason string) { m.dlq.WithLabelValues(reason).Inc() }

func (m *Metrics) OutboxLag(d time.Duration) { m.outboxLag.Set(d.Seconds()) }

func (m *Metrics) OutboxPublished(eventType string) { m.outboxPub.WithLabelValues(eventType).Inc() }

func (m *Metrics) OutboxFailure() { m.outboxFail.Inc() }

func (m *Metrics) SQSMessage(outcome string) { m.sqsMessages.WithLabelValues(outcome).Inc() }

func (m *Metrics) HTTPRequest(route, code string, d time.Duration) {
	m.httpRequests.WithLabelValues(route, code).Inc()
	m.httpDuration.WithLabelValues(route).Observe(d.Seconds())
}
