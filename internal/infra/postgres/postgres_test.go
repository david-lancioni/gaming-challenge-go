package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

func TestMapErrClassifiesTransientFailures(t *testing.T) {
	pg := func(code string) error { return &pgconn.PgError{Code: code, Message: "x"} }
	transient := map[string]error{
		"context canceled":       context.Canceled,
		"deadline":               context.DeadlineExceeded,
		"serialization failure":  pg("40001"),
		"deadlock":               pg("40P01"),
		"lock timeout":           pg("55P03"),
		"statement timeout":      pg("57014"),
		"idle in transaction":    pg("25P03"),
		"connection failure":     pg("08006"),
		"admin shutdown":         pg("57P01"),
		"insufficient resources": pg("53300"),
		"eof":                    io.EOF,
		"closed pool":            errors.New("closed pool"),
		"wrapped deadlock":       fmt.Errorf("tx: %w", pg("40P01")),
	}
	for name, err := range transient {
		got := mapErr(err)
		if !errors.Is(got, application.ErrTransient) {
			t.Errorf("%s must be transient, got %v", name, got)
		}
		if !errors.Is(got, err) {
			t.Errorf("%s: the original error must remain reachable", name)
		}
	}
	permanent := map[string]error{
		"unique violation":    pg("23505"),
		"integrity trigger":   pg("23000"),
		"check violation":     pg("23514"),
		"syntax error":        pg("42601"),
		"domain error":        domain.ErrInsufficientFunds,
		"not found":           domain.ErrWalletNotFound,
		"concurrent modified": application.ErrConcurrentModification,
	}
	for name, err := range permanent {
		if got := mapErr(err); errors.Is(got, application.ErrTransient) {
			t.Errorf("%s must not be transient", name)
		}
	}
	if mapErr(nil) != nil {
		t.Error("nil stays nil")
	}
	already := mapErr(context.Canceled)
	if mapErr(already) != already {
		t.Error("mapping is idempotent")
	}
}

func TestConflictRetryClassification(t *testing.T) {
	for code, want := range map[string]string{"40001": "serialization", "40P01": "deadlock"} {
		reason, ok := isConflictRetryable(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code}))
		if !ok || reason != want {
			t.Errorf("%s -> %q %v", code, reason, ok)
		}
	}
	if _, ok := isConflictRetryable(&pgconn.PgError{Code: "55P03"}); ok {
		t.Error("a lock timeout is surfaced, not retried")
	}
	if !isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "c1"}, "c1") ||
		isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "c2"}, "c1") ||
		isUniqueViolation(&pgconn.PgError{Code: "23514", ConstraintName: "c1"}, "c1") {
		t.Error("unique violation matching")
	}
}

func TestNextAttemptDelayIsRelativeToTheTransition(t *testing.T) {
	base := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	future := base.Add(1500 * time.Millisecond)
	past := base.Add(-time.Hour)
	if d := nextAttemptDelay(domain.TransactionSnapshot{UpdatedAt: base, NextAttemptAt: &future}); d == nil || *d != 1500 {
		t.Errorf("delay = %v", d)
	}
	if d := nextAttemptDelay(domain.TransactionSnapshot{UpdatedAt: base, NextAttemptAt: &past}); d == nil || *d != 0 {
		t.Errorf("a retry in the past is due immediately, got %v", d)
	}
	if nextAttemptDelay(domain.TransactionSnapshot{UpdatedAt: base}) != nil {
		t.Error("terminal transactions have no next attempt")
	}
	if nullable("") != nil || *nullable("x") != "x" || str(nil) != "" {
		t.Error("nullable helpers")
	}
	_ = uuid.Nil
}

func TestLoadMigrations(t *testing.T) {
	good := fstest.MapFS{
		"0002_second.up.sql":   {Data: []byte("SELECT 2;")},
		"0002_second.down.sql": {Data: []byte("SELECT -2;")},
		"0001_first.up.sql":    {Data: []byte("SELECT 1;")},
		"0001_first.down.sql":  {Data: []byte("SELECT -1;")},
		"embed.go":             {Data: []byte("package x")},
		"README.txt":           {Data: []byte("ignored")},
	}
	migs, err := loadMigrations(good)
	if err != nil || len(migs) != 2 || migs[0].version != 1 || migs[1].version != 2 || migs[0].name != "first" || migs[1].down != "SELECT -2;" {
		t.Fatalf("migrations = %+v, %v", migs, err)
	}
	missingDown := fstest.MapFS{"0001_x.up.sql": {Data: []byte("SELECT 1;")}}
	if _, err := loadMigrations(missingDown); err == nil {
		t.Error("a migration without a down script must be refused: every migration has to be reversible")
	}
	missingUp := fstest.MapFS{"0001_x.down.sql": {Data: []byte("SELECT 1;")}}
	if _, err := loadMigrations(missingUp); err == nil {
		t.Error("a migration without an up script must be refused")
	}
}
