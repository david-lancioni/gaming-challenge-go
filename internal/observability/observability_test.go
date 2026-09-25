package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

func TestLoggerEmitsJSONWithContextIdentifiers(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info")
	ctx := With(context.Background(), slog.String("correlationId", "c-1"), slog.String("walletId", "w-1"))
	ctx = With(ctx, slog.String("providerId", "provider-a"))
	log.InfoContext(ctx, "processed", slog.String("status", "PROCESSED"))
	log.DebugContext(ctx, "hidden")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("debug must be filtered at info level: %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	for k, want := range map[string]string{"msg": "processed", "correlationId": "c-1", "walletId": "w-1", "providerId": "provider-a", "status": "PROCESSED"} {
		if rec[k] != want {
			t.Errorf("%s = %v, want %s", k, rec[k], want)
		}
	}
	if got := Attrs(context.Background()); len(got) != 0 {
		t.Errorf("attrs leaked: %v", got)
	}
}

func TestLoggerLevels(t *testing.T) {
	for level, wantDebug := range map[string]bool{"debug": true, "info": false, "warn": false, "error": false, "bogus": false} {
		var buf bytes.Buffer
		NewLogger(&buf, level).Debug("d")
		if (buf.Len() > 0) != wantDebug {
			t.Errorf("level %s: debug emitted = %v", level, buf.Len() > 0)
		}
	}
}

func TestMetricsAreRegisteredAndCounted(t *testing.T) {
	m := NewMetrics()
	m.TransactionResult(application.SourceHTTP, domain.KindBet, domain.StatusRejected, domain.FailureInsufficientFunds)
	m.TransactionResult(application.SourceHTTP, domain.KindBet, domain.StatusRejected, domain.FailureInsufficientFunds)
	m.Duplicate(application.SourceSQS, "replay")
	m.Retry("sqs_consumer")
	m.DeadLetter("invalid_message")
	m.ConcurrencyConflict("deadlock")
	m.ReconciliationDivergence()
	m.OutboxLag(3 * time.Second)
	m.OutboxPublished("WalletBalanceChanged")
	m.OutboxFailure()
	m.SQSMessage("processed")
	m.WalletLockWait(time.Millisecond)
	m.ProcessingDuration(application.SourceHTTP, time.Millisecond)
	m.HTTPRequest("GET /x", "2xx", time.Millisecond)

	if got := testutil.ToFloat64(m.transactions.WithLabelValues("http", "BET", "REJECTED", "INSUFFICIENT_FUNDS")); got != 2 {
		t.Errorf("transactions = %v", got)
	}
	if got := testutil.ToFloat64(m.outboxLag); got != 3 {
		t.Errorf("outbox lag = %v", got)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{
		"wagering_transactions_total", "wagering_duplicates_total", "wagering_retries_total", "wagering_dlq_messages_total",
		"wagering_concurrency_conflicts_total", "wagering_outbox_lag_seconds", "wagering_processing_duration_seconds",
		"wagering_reconciliation_divergences_total", "wagering_wallet_lock_wait_seconds", "wagering_outbox_published_total",
	} {
		if !names[want] {
			t.Errorf("metric %s not exposed", want)
		}
	}
}
