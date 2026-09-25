package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/application"
)

const maxConflictRetries = 3

type UnitOfWork struct {
	pool    *pgxpool.Pool
	metrics application.Metrics
}

func NewUnitOfWork(pool *pgxpool.Pool, metrics application.Metrics) *UnitOfWork {
	if metrics == nil {
		metrics = application.NopMetrics{}
	}
	return &UnitOfWork{pool: pool, metrics: metrics}
}

var _ application.UnitOfWork = (*UnitOfWork)(nil)

// Do executa fn numa transação READ COMMITTED; commit se fn retornar nil, rollback
// caso contrário. READ COMMITTED basta porque a consistência vem do lock explícito
// da carteira (FOR UPDATE) e das constraints. Deadlock e falha de serialização são
// reexecutados algumas vezes; lock timeout vira application.ErrTransient.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, r application.Repositories) error) error {
	for attempt := 0; ; attempt++ {
		err := u.runOnce(ctx, fn)
		if err == nil {
			return nil
		}
		if reason, ok := isConflictRetryable(err); ok {
			u.metrics.ConcurrencyConflict(reason)
			if attempt < maxConflictRetries {
				select {
				case <-time.After(application.Backoff(5*time.Millisecond, 100*time.Millisecond, attempt)):
					continue
				case <-ctx.Done():
					return mapErr(ctx.Err())
				}
			}
		} else if pe := pgError(err); pe != nil && pe.Code == codeLockNotAvailable {
			u.metrics.ConcurrencyConflict("lock_timeout")
		}
		return mapErr(err)
	}
}

func (u *UnitOfWork) runOnce(ctx context.Context, fn func(ctx context.Context, r application.Repositories) error) (err error) {
	tx, err := u.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Rollback com contexto próprio: o do chamador pode já estar cancelado
		// (timeout, shutdown) e a conexão não pode voltar ao pool com a transação aberta.
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) && err == nil {
			err = rbErr
		}
	}()
	if err = fn(ctx, newTxRepos(tx)); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

type txRepos struct {
	wallets      *walletRepo
	transactions *transactionRepo
	ledger       *ledgerRepo
	outbox       *outboxRepo
	inbox        *inboxRepo
}

func newTxRepos(db dbtx) *txRepos {
	return &txRepos{
		wallets:      &walletRepo{db: db},
		transactions: &transactionRepo{db: db},
		ledger:       &ledgerRepo{db: db},
		outbox:       &outboxRepo{db: db},
		inbox:        &inboxRepo{db: db},
	}
}

func (t *txRepos) Wallets() application.WalletRepository           { return t.wallets }
func (t *txRepos) Transactions() application.TransactionRepository { return t.transactions }
func (t *txRepos) Ledger() application.LedgerRepository            { return t.ledger }
func (t *txRepos) Outbox() application.OutboxRepository            { return t.outbox }
func (t *txRepos) Inbox() application.InboxRepository              { return t.inbox }
