package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

type WalletService struct {
	uow     UnitOfWork
	reader  WalletReader
	clock   Clock
	metrics Metrics
	log     *slog.Logger
}

func NewWalletService(uow UnitOfWork, reader WalletReader, clock Clock, metrics Metrics, log *slog.Logger) *WalletService {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &WalletService{uow: uow, reader: reader, clock: clock, metrics: metrics, log: log}
}

func (s *WalletService) Open(ctx context.Context, playerID string, initial domain.Money, correlationID string) (*domain.Wallet, error) {
	pid, err := domain.ParseID("playerId", playerID)
	if err != nil {
		return nil, err
	}
	var wallet *domain.Wallet
	err = s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		opening, err := domain.OpenWallet(pid, initial, correlationID, s.clock())
		if err != nil {
			return err
		}
		if err := r.Wallets().Insert(ctx, opening.Wallet); err != nil {
			return err
		}
		if opening.Transaction != nil {
			if err := r.Transactions().Insert(ctx, opening.Transaction); err != nil {
				return err
			}
			if err := r.Ledger().Insert(ctx, *opening.Entry); err != nil {
				return err
			}
			if err := r.Outbox().Add(ctx, opening.Events...); err != nil {
				return err
			}
		}
		wallet = opening.Wallet
		return nil
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return s.reader.Get(ctx, id)
}

type LedgerPage struct {
	Entries    []domain.LedgerEntry
	NextCursor string
}

type cursorPayload struct {
	Before int64 `json:"b"`
}

func encodeCursor(beforeVersion int64) string {
	raw, _ := json.Marshal(cursorPayload{Before: beforeVersion})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil || p.Before < 1 {
		return 0, fmt.Errorf("%w: malformed", ErrInvalidCursor)
	}
	return p.Before, nil
}

func (s *WalletService) Ledger(ctx context.Context, id uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if limit == 0 {
		limit = DefaultLedgerLimit
	}
	if limit < 1 || limit > MaxLedgerLimit {
		return LedgerPage{}, domain.NewValidationError("limit", fmt.Sprintf("must be between 1 and %d", MaxLedgerLimit))
	}
	var before *int64
	if cursor != "" {
		v, err := decodeCursor(cursor)
		if err != nil {
			return LedgerPage{}, err
		}
		before = &v
	}
	if _, err := s.reader.Get(ctx, id); err != nil {
		return LedgerPage{}, err
	}
	entries, err := s.reader.ListLedger(ctx, id, before, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Entries: entries}
	if len(entries) > limit {
		page.Entries = entries[:limit]
		page.NextCursor = encodeCursor(page.Entries[limit-1].WalletVersion())
	}
	return page, nil
}

type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int64
}

func (s *WalletService) Reconcile(ctx context.Context, id uuid.UUID) (Reconciliation, error) {
	sum, err := s.reader.Reconciliation(ctx, id)
	if err != nil {
		return Reconciliation{}, err
	}
	diff, err := sum.Stored.Sub(sum.Calculated)
	if err != nil {
		return Reconciliation{}, err
	}
	res := Reconciliation{
		WalletID: id, StoredBalance: sum.Stored, CalculatedBalance: sum.Calculated,
		Difference: diff, Consistent: diff.IsZero(), CheckedEntries: sum.Entries,
	}
	if !res.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.ErrorContext(ctx, "wallet reconciliation divergence",
			slog.String("walletId", id.String()),
			slog.String("stored", sum.Stored.Amount()),
			slog.String("calculated", sum.Calculated.Amount()),
			slog.String("difference", diff.Amount()),
			slog.Int64("checkedEntries", sum.Entries))
	}
	return res, nil
}

type PendingResolver struct {
	uow     UnitOfWork
	queue   PendingQueue
	proc    *Processor
	clock   Clock
	metrics Metrics
	log     *slog.Logger
	lease   time.Duration
}

func NewPendingResolver(uow UnitOfWork, queue PendingQueue, proc *Processor, clock Clock, lease time.Duration, metrics Metrics, log *slog.Logger) *PendingResolver {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &PendingResolver{uow: uow, queue: queue, proc: proc, clock: clock, lease: lease, metrics: metrics, log: log}
}

func (p *PendingResolver) RunOnce(ctx context.Context, limit int) (int, error) {
	due, err := p.queue.ClaimDue(ctx, limit, p.lease)
	if err != nil {
		return 0, err
	}
	for _, d := range due {
		if ctx.Err() != nil {
			return len(due), ctx.Err()
		}
		p.resolveOne(ctx, d)
	}
	return len(due), nil
}

func (p *PendingResolver) resolveOne(ctx context.Context, d DueTransaction) {
	start := time.Now()
	var txn *domain.WagerTransaction
	err := p.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		txn, err = p.proc.ResolvePending(ctx, r, d)
		return err
	})
	log := p.log.With(slog.String("transactionId", d.ID.String()), slog.String("walletId", d.WalletID.String()))
	switch {
	case err == nil:
		if txn != nil {
			p.proc.RecordOutcome(SourceWorker, Outcome{Transaction: txn}, time.Since(start))
			log.InfoContext(ctx, "pending operation evaluated", slog.String("status", string(txn.Status())),
				slog.Int("attempts", txn.Attempts()), slog.String("failureCode", string(txn.FailureCode())))
		}
	case errors.Is(err, ErrTransient) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrConcurrentModification):
		p.metrics.Retry("reference_worker_transient")
		log.WarnContext(ctx, "pending operation deferred after a transient failure", slog.String("error", err.Error()))
	default:
		log.ErrorContext(ctx, "pending operation failed permanently", slog.String("error", err.Error()))
		cause := err
		ferr := p.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
			_, ferr := p.proc.FailPending(ctx, r, d, cause)
			return ferr
		})
		if ferr != nil {
			log.ErrorContext(ctx, "could not record permanent failure", slog.String("error", ferr.Error()))
		} else {
			p.metrics.TransactionResult(SourceWorker, "", domain.StatusFailed, domain.FailureProcessingError)
		}
	}
}
