package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const migrationLockKey int64 = 7_270_001

var migrationFile = regexp.MustCompile(`^(\d+)_([a-z0-9_]+)\.(up|down)\.sql$`)

type migration struct {
	version int64
	name    string
	up      string
	down    string
}

type AppliedMigration struct {
	Version int64
	Name    string
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	byVersion := map[int64]*migration{}
	for _, e := range entries {
		m := migrationFile.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		version, _ := strconv.ParseInt(m[1], 10, 64)
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		mig := byVersion[version]
		if mig == nil {
			mig = &migration{version: version, name: m[2]}
			byVersion[version] = mig
		}
		if m[3] == "up" {
			mig.up = string(body)
		} else {
			mig.down = string(body)
		}
	}
	out := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.up == "" || m.down == "" {
			return nil, fmt.Errorf("migration %d_%s needs both .up.sql and .down.sql", m.version, m.name)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

type Migrator struct {
	conn *pgx.Conn
	fsys fs.FS
}

func NewMigrator(ctx context.Context, url string, fsys fs.FS) (*Migrator, error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	m := &Migrator{conn: conn, fsys: fsys}
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version bigint PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}
	return m, nil
}

func (m *Migrator) Close(ctx context.Context) error { return m.conn.Close(ctx) }

func (m *Migrator) lock(ctx context.Context) error {
	_, err := m.conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey)
	return err
}

func (m *Migrator) unlock(ctx context.Context) {
	_, _ = m.conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockKey)
}

func (m *Migrator) applied(ctx context.Context) (map[int64]bool, []AppliedMigration, error) {
	rows, err := m.conn.Query(ctx, `SELECT version, name FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	set := map[int64]bool{}
	var list []AppliedMigration
	for rows.Next() {
		var a AppliedMigration
		if err := rows.Scan(&a.Version, &a.Name); err != nil {
			return nil, nil, err
		}
		set[a.Version] = true
		list = append(list, a)
	}
	return set, list, rows.Err()
}

func (m *Migrator) Up(ctx context.Context) ([]int64, error) {
	migs, err := loadMigrations(m.fsys)
	if err != nil {
		return nil, err
	}
	if err := m.lock(ctx); err != nil {
		return nil, err
	}
	defer m.unlock(ctx)
	done, _, err := m.applied(ctx)
	if err != nil {
		return nil, err
	}
	var applied []int64
	for _, mig := range migs {
		if done[mig.version] {
			continue
		}
		if err := m.run(ctx, mig.up, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, mig); err != nil {
			return applied, fmt.Errorf("apply %04d_%s: %w", mig.version, mig.name, err)
		}
		applied = append(applied, mig.version)
	}
	return applied, nil
}

func (m *Migrator) Down(ctx context.Context, steps int) ([]int64, error) {
	migs, err := loadMigrations(m.fsys)
	if err != nil {
		return nil, err
	}
	if err := m.lock(ctx); err != nil {
		return nil, err
	}
	defer m.unlock(ctx)
	_, list, err := m.applied(ctx)
	if err != nil {
		return nil, err
	}
	byVersion := map[int64]migration{}
	for _, mig := range migs {
		byVersion[mig.version] = mig
	}
	var reverted []int64
	for i := len(list) - 1; i >= 0 && len(reverted) < steps; i-- {
		mig, ok := byVersion[list[i].Version]
		if !ok {
			return reverted, fmt.Errorf("applied migration %d has no files", list[i].Version)
		}
		if err := m.run(ctx, mig.down, `DELETE FROM schema_migrations WHERE version = $1`, mig); err != nil {
			return reverted, fmt.Errorf("revert %04d_%s: %w", mig.version, mig.name, err)
		}
		reverted = append(reverted, mig.version)
	}
	return reverted, nil
}

func (m *Migrator) Applied(ctx context.Context) ([]AppliedMigration, error) {
	_, list, err := m.applied(ctx)
	return list, err
}

func (m *Migrator) run(ctx context.Context, script, bookkeeping string, mig migration) error {
	tx, err := m.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, script); err != nil {
		return err
	}
	args := []any{mig.version}
	if bookkeeping[:6] == "INSERT" {
		args = append(args, mig.name)
	}
	if _, err := tx.Exec(ctx, bookkeeping, args...); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
