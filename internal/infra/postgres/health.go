package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Checker struct{ Pool *pgxpool.Pool }

func (c *Checker) Name() string { return "postgres" }

func (c *Checker) Check(ctx context.Context) error { return c.Pool.Ping(ctx) }

func VerifySchema(ctx context.Context, pool *pgxpool.Pool) error {
	var ok bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.wager_transactions') IS NOT NULL
		AND to_regclass('public.outbox_events') IS NOT NULL`).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return errors.New("database schema is not migrated: run `wagering migrate up`")
	}
	return nil
}
