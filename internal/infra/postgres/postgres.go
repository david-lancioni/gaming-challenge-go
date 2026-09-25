package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/application"
)

type PoolConfig struct {
	URL              string
	MaxConns         int32
	MinConns         int32
	LockTimeout      time.Duration
	StatementTimeout time.Duration
	IdleInTxTimeout  time.Duration
	ApplicationName  string
}

func NewPool(ctx context.Context, c PoolConfig) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(c.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if c.MaxConns > 0 {
		cfg.MaxConns = c.MaxConns
	}
	if c.MinConns > 0 {
		cfg.MinConns = c.MinConns
	}
	rp := cfg.ConnConfig.RuntimeParams
	ms := func(d time.Duration) string { return fmt.Sprintf("%d", d.Milliseconds()) }
	if c.LockTimeout > 0 {
		rp["lock_timeout"] = ms(c.LockTimeout)
	}
	if c.StatementTimeout > 0 {
		rp["statement_timeout"] = ms(c.StatementTimeout)
	}
	if c.IdleInTxTimeout > 0 {
		rp["idle_in_transaction_session_timeout"] = ms(c.IdleInTxTimeout)
	}
	if c.ApplicationName != "" {
		rp["application_name"] = c.ApplicationName
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const (
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
	codeLockNotAvailable     = "55P03"
	codeQueryCanceled        = "57014"
	codeUniqueViolation      = "23505"
)

func pgError(err error) *pgconn.PgError {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

func isConflictRetryable(err error) (reason string, ok bool) {
	if pe := pgError(err); pe != nil {
		switch pe.Code {
		case codeSerializationFailure:
			return "serialization", true
		case codeDeadlockDetected:
			return "deadlock", true
		}
	}
	return "", false
}

func isUniqueViolation(err error, constraint string) bool {
	pe := pgError(err)
	return pe != nil && pe.Code == codeUniqueViolation && pe.ConstraintName == constraint
}

func mapErr(err error) error {
	if err == nil || errors.Is(err, application.ErrTransient) {
		return err
	}
	transient := false
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		transient = true
	case pgError(err) != nil:
		code := pgError(err).Code
		switch {
		case code == codeSerializationFailure, code == codeDeadlockDetected, code == codeLockNotAvailable, code == codeQueryCanceled,
			code == "25P03":
			transient = true
		case strings.HasPrefix(code, "08"), strings.HasPrefix(code, "53"), strings.HasPrefix(code, "57P"):
			transient = true
		}
	default:
		var netErr net.Error
		var connErr *pgconn.ConnectError
		switch {
		case errors.As(err, &netErr), errors.As(err, &connErr),
			errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
			pgconn.Timeout(err), strings.Contains(err.Error(), "closed pool"):
			transient = true
		}
	}
	if transient {
		return fmt.Errorf("%w: %w", application.ErrTransient, err)
	}
	return err
}
