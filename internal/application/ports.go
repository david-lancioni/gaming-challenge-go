package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

var (
	ErrTransient              = errors.New("transient infrastructure failure")
	ErrDuplicateTransaction   = errors.New("duplicate transaction")
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrInvalidCursor          = errors.New("invalid cursor")
)

type Clock func() time.Time

type Source string

const (
	SourceHTTP   Source = "http"
	SourceSQS    Source = "sqs"
	SourceWorker Source = "worker"
)

type Repositories interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
}

type WalletRepository interface {
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	Insert(ctx context.Context, w *domain.Wallet) error
	UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error
}

type WalletReader interface {
	Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	Reconciliation(ctx context.Context, id uuid.UUID) (LedgerSummary, error)
	ListLedger(ctx context.Context, id uuid.UUID, beforeVersion *int64, limit int) ([]domain.LedgerEntry, error)
}

type LedgerSummary struct {
	Stored     domain.Money
	Calculated domain.Money
	Entries    int64
}

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	FindByExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	FindByProviderKeys(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*domain.WagerTransaction, error)
}

type TransactionRepository interface {
	TransactionReader
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	Update(ctx context.Context, t *domain.WagerTransaction) error
	HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	WakeDependents(ctx context.Context, providerID, externalID string) error
}

type DueTransaction struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

type PendingQueue interface {
	ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]DueTransaction, error)
}

type LedgerRepository interface {
	Insert(ctx context.Context, e domain.LedgerEntry) error
}

type OutboxRepository interface {
	Add(ctx context.Context, events ...domain.Event) error
}

type InboxRepository interface {
	Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (inserted bool, existingHash string, err error)
	Complete(ctx context.Context, consumer, messageID string, now time.Time) error
}

type OutboxRecord struct {
	ID           uuid.UUID
	EventType    string
	AggregateID  uuid.UUID
	PartitionKey uuid.UUID
	Payload      []byte
	OccurredAt   time.Time
	Attempts     int
}

type OutboxQueue interface {
	Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, cause string, retryAfter time.Duration) error
	OldestUnpublishedAge(ctx context.Context) (time.Duration, error)
}

type Metrics interface {
	TransactionResult(source Source, kind domain.TransactionKind, status domain.TransactionStatus, code domain.FailureCode)
	Duplicate(source Source, outcome string)
	ProcessingDuration(source Source, d time.Duration)
	ConcurrencyConflict(reason string)
	WalletLockWait(d time.Duration)
	ReconciliationDivergence()
	Retry(component string)
}

type NopMetrics struct{}

func (NopMetrics) TransactionResult(Source, domain.TransactionKind, domain.TransactionStatus, domain.FailureCode) {
}
func (NopMetrics) Duplicate(Source, string)                 {}
func (NopMetrics) ProcessingDuration(Source, time.Duration) {}
func (NopMetrics) ConcurrencyConflict(string)               {}
func (NopMetrics) WalletLockWait(time.Duration)             {}
func (NopMetrics) ReconciliationDivergence()                {}
func (NopMetrics) Retry(string)                             {}
