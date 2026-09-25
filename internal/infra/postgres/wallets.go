package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

type walletRepo struct {
	db   dbtx
	pool *pgxpool.Pool
}

func NewWalletReader(pool *pgxpool.Pool) application.WalletReader {
	return &walletRepo{db: pool, pool: pool}
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		id, player       uuid.UUID
		currency         string
		balance, version int64
		created, updated time.Time
	)
	if err := row.Scan(&id, &player, &currency, &balance, &version, &created, &updated); err != nil {
		return nil, err
	}
	money, err := domain.MoneyFromMinor(balance, domain.Currency(currency))
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(id, player, money, version, created, updated)
}

func (r *walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := scanWallet(r.db.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
	return w, walletErr(err)
}

func (r *walletRepo) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := scanWallet(r.db.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
	return w, mapErr(walletErr(err))
}

func walletErr(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrWalletNotFound
	}
	return err
}

func (r *walletRepo) Insert(ctx context.Context, w *domain.Wallet) error {
	_, err := r.db.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return domain.ErrWalletAlreadyExists
	}
	return err
}

// UpdateBalance grava saldo e versão condicionado à versão lida sob lock
// (WHERE version = expected). É uma segunda barreira contra lost update, além do
// FOR UPDATE: se outro escritor tivesse alterado a carteira, nenhuma linha seria
// afetada e a operação falharia em vez de sobrescrever o saldo.
func (r *walletRepo) UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrConcurrentModification
	}
	return nil
}

func (r *walletRepo) ListLedger(ctx context.Context, id uuid.UUID, beforeVersion *int64, limit int) ([]domain.LedgerEntry, error) {
	rows, err := r.db.Query(ctx, `SELECT `+ledgerColumns+` FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND ($2::bigint IS NULL OR wallet_version < $2)
		ORDER BY wallet_version DESC LIMIT $3`, id, beforeVersion, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var entries []domain.LedgerEntry
	for rows.Next() {
		e, err := scanLedgerEntry(rows)
		if err != nil {
			return nil, mapErr(err)
		}
		entries = append(entries, e)
	}
	return entries, mapErr(rows.Err())
}

// Reconciliation lê o saldo armazenado e o saldo reconstruído do ledger no mesmo
// snapshot (REPEATABLE READ, somente leitura). Assim uma movimentação concorrente
// não é vista pela metade nem faz uma carteira correta parecer divergente.
func (r *walletRepo) Reconciliation(ctx context.Context, id uuid.UUID) (application.LedgerSummary, error) {
	if r.pool == nil {
		return application.LedgerSummary{}, errors.New("reconciliation requires a pool-bound reader")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return application.LedgerSummary{}, mapErr(err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	wallet, err := scanWallet(tx.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
	if err != nil {
		return application.LedgerSummary{}, mapErr(walletErr(err))
	}
	var entries int64
	var sum string
	// SUM de bigint resulta em numeric (sem overflow no banco); vem como texto e é
	// convertido para inteiro, nunca passando por float.
	err = tx.QueryRow(ctx, `SELECT count(*), COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::text
		FROM wallet_ledger_entries WHERE wallet_id = $1`, id).Scan(&entries, &sum)
	if err != nil {
		return application.LedgerSummary{}, mapErr(err)
	}
	minor, err := strconv.ParseInt(sum, 10, 64)
	if err != nil {
		return application.LedgerSummary{}, fmt.Errorf("ledger sum %s does not fit in Money: %w", sum, domain.ErrMoneyOverflow)
	}
	calculated, err := domain.MoneyFromMinor(minor, wallet.Currency())
	if err != nil {
		return application.LedgerSummary{}, err
	}
	return application.LedgerSummary{Stored: wallet.Balance(), Calculated: calculated, Entries: entries}, nil
}

const ledgerColumns = `id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, wallet_version, created_at`

func scanLedgerEntry(row pgx.Row) (domain.LedgerEntry, error) {
	var (
		id, walletID, txID    uuid.UUID
		direction, currency   string
		amount, before, after int64
		version               int64
		created               time.Time
	)
	if err := row.Scan(&id, &walletID, &txID, &direction, &amount, &currency, &before, &after, &version, &created); err != nil {
		return domain.LedgerEntry{}, err
	}
	cur := domain.Currency(currency)
	m, err := domain.MoneyFromMinor(amount, cur)
	if err != nil {
		return domain.LedgerEntry{}, err
	}
	b, err := domain.MoneyFromMinor(before, cur)
	if err != nil {
		return domain.LedgerEntry{}, err
	}
	a, err := domain.MoneyFromMinor(after, cur)
	if err != nil {
		return domain.LedgerEntry{}, err
	}
	return domain.RehydrateLedgerEntry(id, walletID, txID, domain.Direction(direction), m, b, a, version, created)
}

type ledgerRepo struct{ db dbtx }

func (r *ledgerRepo) Insert(ctx context.Context, e domain.LedgerEntry) error {
	_, err := r.db.Exec(ctx, `INSERT INTO wallet_ledger_entries (`+ledgerColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Money().Minor(), string(e.Money().Currency()),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.WalletVersion(), e.CreatedAt())
	return err
}
