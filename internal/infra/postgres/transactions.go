package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

const txnColumns = `id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
	reference_external_transaction_id, reference_transaction_id, status, failure_code, failure_message,
	result_balance_minor, attempts, next_attempt_at, expires_at, correlation_id, causation_id,
	created_at, updated_at, processed_at`

type transactionRepo struct {
	db     dbtx
	reader bool
}

func NewTransactionReader(pool *pgxpool.Pool) application.TransactionReader {
	return &transactionRepo{db: pool, reader: true}
}

func (r *transactionRepo) wrap(err error) error {
	if r.reader {
		return mapErr(err)
	}
	return err
}

func scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id, walletID, playerID                uuid.UUID
		origin, kind, status, currency        string
		providerID, externalID, idemKey, hash *string
		roundID, gameID, refExternal          *string
		refID                                 *uuid.UUID
		failureCode, failureMsg, corr, cause  *string
		amount                                int64
		resultBalance                         *int64
		attempts                              int
		nextAttempt, expires, processed       *time.Time
		created, updated                      time.Time
	)
	if err := row.Scan(&id, &origin, &providerID, &externalID, &idemKey, &hash, &walletID, &playerID, &roundID, &gameID,
		&kind, &amount, &currency, &refExternal, &refID, &status, &failureCode, &failureMsg, &resultBalance,
		&attempts, &nextAttempt, &expires, &corr, &cause, &created, &updated, &processed); err != nil {
		return nil, err
	}
	cur := domain.Currency(currency)
	money, err := domain.MoneyFromMinor(amount, cur)
	if err != nil {
		return nil, err
	}
	var result *domain.Money
	if resultBalance != nil {
		m, err := domain.MoneyFromMinor(*resultBalance, cur)
		if err != nil {
			return nil, err
		}
		result = &m
	}
	utc := func(t *time.Time) *time.Time {
		if t == nil {
			return nil
		}
		u := t.UTC()
		return &u
	}
	return domain.RehydrateTransaction(domain.TransactionSnapshot{
		ID: id, Origin: domain.Origin(origin), ProviderID: str(providerID), ExternalTransactionID: str(externalID),
		IdempotencyKey: str(idemKey), PayloadHash: str(hash), WalletID: walletID, PlayerID: playerID,
		RoundID: str(roundID), GameID: str(gameID), Kind: domain.TransactionKind(kind), Money: money,
		ReferenceExternalTransactionID: str(refExternal), ReferenceTransactionID: refID,
		Status: domain.TransactionStatus(status), FailureCode: domain.FailureCode(str(failureCode)),
		FailureMessage: str(failureMsg), ResultBalance: result, Attempts: attempts,
		NextAttemptAt: utc(nextAttempt), ExpiresAt: utc(expires), CorrelationID: str(corr), CausationID: str(cause),
		CreatedAt: created, UpdatedAt: updated, ProcessedAt: utc(processed),
	})
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (r *transactionRepo) one(ctx context.Context, query string, args ...any) (*domain.WagerTransaction, error) {
	t, err := scanTransaction(r.db.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrTransactionNotFound
	}
	return t, r.wrap(err)
}

func (r *transactionRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return r.one(ctx, `SELECT `+txnColumns+` FROM wager_transactions WHERE id = $1`, id)
}

func (r *transactionRepo) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return r.one(ctx, `SELECT `+txnColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id)
}

func (r *transactionRepo) FindByExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return r.one(ctx, `SELECT `+txnColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID)
}

func (r *transactionRepo) FindByProviderKeys(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*domain.WagerTransaction, error) {
	rows, err := r.db.Query(ctx, `SELECT `+txnColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, idempotencyKey, externalID)
	if err != nil {
		return nil, r.wrap(err)
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, r.wrap(err)
		}
		out = append(out, t)
	}
	return out, r.wrap(rows.Err())
}

// nextAttemptDelay converte o instante absoluto calculado pelo domínio (relógio da
// aplicação) num atraso relativo. O banco agenda com o próprio relógio, a única
// referência de tempo compartilhada por todas as instâncias, então diferenças de
// relógio entre instâncias não atrasam nem adiantam as novas tentativas.
func nextAttemptDelay(s domain.TransactionSnapshot) *int64 {
	if s.NextAttemptAt == nil {
		return nil
	}
	d := s.NextAttemptAt.Sub(s.UpdatedAt).Milliseconds()
	if d < 0 {
		d = 0
	}
	return &d
}

const nextAttemptExpr = `CASE WHEN %s::bigint IS NULL THEN NULL ELSE now() + %s::bigint * interval '1 millisecond' END`

func (r *transactionRepo) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	var result *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.Minor()
		result = &v
	}
	_, err := r.db.Exec(ctx, `INSERT INTO wager_transactions (`+txnColumns+`) VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, `+fmt.Sprintf(nextAttemptExpr, "$21", "$21")+`, $22, $23, $24, $25, $26, $27)`,
		s.ID, string(s.Origin), nullable(s.ProviderID), nullable(s.ExternalTransactionID), nullable(s.IdempotencyKey), nullable(s.PayloadHash),
		s.WalletID, s.PlayerID, nullable(s.RoundID), nullable(s.GameID), string(s.Kind), s.Money.Minor(), string(s.Money.Currency()),
		nullable(s.ReferenceExternalTransactionID), s.ReferenceTransactionID, string(s.Status), nullable(string(s.FailureCode)),
		nullable(s.FailureMessage), result, s.Attempts, nextAttemptDelay(s), s.ExpiresAt, nullable(s.CorrelationID), nullable(s.CausationID),
		s.CreatedAt, s.UpdatedAt, s.ProcessedAt)
	// O banco é a última barreira destas regras; chegar aqui significa que outro
	// escritor gravou primeiro. O chamador reavalia a operação do zero.
	switch {
	case isUniqueViolation(err, "wager_tx_provider_external_key"), isUniqueViolation(err, "wager_tx_provider_idempotency_key"):
		return application.ErrDuplicateTransaction
	case isUniqueViolation(err, "wager_tx_one_reversal_per_reference"), isUniqueViolation(err, "wager_tx_one_opening_per_wallet"):
		return fmt.Errorf("%w: %w", application.ErrConcurrentModification, err)
	}
	return err
}

func (r *transactionRepo) Update(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	var result *int64
	if s.ResultBalance != nil {
		v := s.ResultBalance.Minor()
		result = &v
	}
	tag, err := r.db.Exec(ctx, `UPDATE wager_transactions SET
			status = $2, failure_code = $3, failure_message = $4, result_balance_minor = $5,
			reference_transaction_id = $6, attempts = $7, next_attempt_at = `+fmt.Sprintf(nextAttemptExpr, "$8", "$8")+`, expires_at = $9,
			updated_at = $10, processed_at = $11
		WHERE id = $1 AND status IN ('PENDING', 'PENDING_REFERENCE')`,
		s.ID, string(s.Status), nullable(string(s.FailureCode)), nullable(s.FailureMessage), result,
		s.ReferenceTransactionID, s.Attempts, nextAttemptDelay(s), s.ExpiresAt, s.UpdatedAt, s.ProcessedAt)
	if isUniqueViolation(err, "wager_tx_one_reversal_per_reference") {
		return fmt.Errorf("%w: %w", application.ErrConcurrentModification, err)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrConcurrentModification
	}
	return nil
}

func (r *transactionRepo) HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM wager_transactions
		 WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`, referenceID).Scan(&exists)
	return exists, err
}

func (r *transactionRepo) WakeDependents(ctx context.Context, providerID, externalID string) error {
	_, err := r.db.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = now()
		WHERE status = 'PENDING_REFERENCE' AND provider_id = $1 AND reference_external_transaction_id = $2
		  AND next_attempt_at > now()`, providerID, externalID)
	return err
}

type pendingQueue struct{ pool *pgxpool.Pool }

func NewPendingQueue(pool *pgxpool.Pool) application.PendingQueue { return &pendingQueue{pool: pool} }

// ClaimDue reserva pendências vencidas com FOR UPDATE SKIP LOCKED, para que
// instâncias concorrentes recebam conjuntos disjuntos, e empurra next_attempt_at
// para a frente pelo tempo do lease: se a instância morrer, a pendência volta a
// vencer sozinha e outra instância a assume. O tempo é o relógio do banco.
func (q *pendingQueue) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]application.DueTransaction, error) {
	rows, err := q.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM wager_transactions
			 WHERE status IN ('PENDING', 'PENDING_REFERENCE') AND next_attempt_at <= now()
			 ORDER BY next_attempt_at
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED)
		UPDATE wager_transactions t
		   SET next_attempt_at = now() + $2::bigint * interval '1 millisecond'
		  FROM due
		 WHERE t.id = due.id
		RETURNING t.id, t.wallet_id`, limit, lease.Milliseconds())
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []application.DueTransaction
	for rows.Next() {
		var d application.DueTransaction
		if err := rows.Scan(&d.ID, &d.WalletID); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, d)
	}
	return out, mapErr(rows.Err())
}
