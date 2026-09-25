package memory

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

type walletRow struct {
	id, player       uuid.UUID
	currency         domain.Currency
	balance, version int64
	created, updated time.Time
}

type state struct {
	wallets   map[uuid.UUID]walletRow
	txns      map[uuid.UUID]domain.TransactionSnapshot
	ledger    []domain.LedgerEntry
	outbox    []StoredEvent
	inbox     map[string]inboxRow
	leases    map[uuid.UUID]time.Time
	reversals map[uuid.UUID]bool
}

type inboxRow struct {
	hash      string
	completed bool
}

type StoredEvent struct {
	ID        uuid.UUID
	Type      domain.EventType
	Aggregate uuid.UUID
	Partition uuid.UUID
	Payload   []byte
}

func (s *state) clone() *state {
	c := &state{
		wallets: map[uuid.UUID]walletRow{}, txns: map[uuid.UUID]domain.TransactionSnapshot{}, inbox: map[string]inboxRow{},
		leases: map[uuid.UUID]time.Time{}, reversals: map[uuid.UUID]bool{},
	}
	for k, v := range s.wallets {
		c.wallets[k] = v
	}
	for k, v := range s.txns {
		c.txns[k] = v
	}
	for k, v := range s.inbox {
		c.inbox[k] = v
	}
	for k, v := range s.leases {
		c.leases[k] = v
	}
	for k, v := range s.reversals {
		c.reversals[k] = v
	}
	c.ledger = append(c.ledger, s.ledger...)
	c.outbox = append(c.outbox, s.outbox...)
	return c
}

type Store struct {
	mu  sync.Mutex
	st  *state
	Now func() time.Time
}

func NewStore() *Store {
	return &Store{st: (&state{}).clone(), Now: time.Now}
}

var _ application.UnitOfWork = (*Store)(nil)

func (s *Store) Do(ctx context.Context, fn func(ctx context.Context, r application.Repositories) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	work := s.st.clone()
	if err := fn(ctx, &repos{s: work, db: s}); err != nil {
		return err
	}
	s.st = work
	return nil
}

func (s *Store) Wallet(id uuid.UUID) (*domain.Wallet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&repos{s: s.st, db: s}).wallets().Get(context.Background(), id)
}

func (s *Store) Ledger() []domain.LedgerEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.LedgerEntry(nil), s.st.ledger...)
}

func (s *Store) Events() []StoredEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StoredEvent(nil), s.st.outbox...)
}

func (s *Store) Transaction(provider, external string) (*domain.WagerTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return (&repos{s: s.st, db: s}).transactions().FindByExternal(context.Background(), provider, external)
}

func (s *Store) Corrupt(id uuid.UUID, balanceMinor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.st.wallets[id]
	w.balance = balanceMinor
	s.st.wallets[id] = w
}

func (s *Store) WalletReader() application.WalletReader           { return &lockedReader{s: s} }
func (s *Store) TransactionReader() application.TransactionReader { return &lockedReader{s: s} }
func (s *Store) PendingQueue() application.PendingQueue           { return &lockedReader{s: s} }

type repos struct {
	s  *state
	db *Store
}

func (r *repos) Wallets() application.WalletRepository           { return r.wallets() }
func (r *repos) Transactions() application.TransactionRepository { return r.transactions() }
func (r *repos) Ledger() application.LedgerRepository            { return &ledgerRepo{r.s} }
func (r *repos) Outbox() application.OutboxRepository            { return &outboxRepo{r.s} }
func (r *repos) Inbox() application.InboxRepository              { return &inboxRepo{r.s} }
func (r *repos) wallets() *walletRepo                            { return &walletRepo{r.s} }
func (r *repos) transactions() *txnRepo                          { return &txnRepo{s: r.s, db: r.db} }

type lockedReader struct{ s *Store }

func (l *lockedReader) with(fn func(r *repos)) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	fn(&repos{s: l.s.st, db: l.s})
}

func (l *lockedReader) Get(ctx context.Context, id uuid.UUID) (w *domain.Wallet, err error) {
	l.with(func(r *repos) { w, err = r.wallets().Get(ctx, id) })
	return
}

func (l *lockedReader) Reconciliation(ctx context.Context, id uuid.UUID) (sum application.LedgerSummary, err error) {
	l.with(func(r *repos) {
		var w *domain.Wallet
		if w, err = r.wallets().Get(ctx, id); err != nil {
			return
		}
		var calc int64
		var n int64
		for _, e := range r.s.ledger {
			if e.WalletID() != id {
				continue
			}
			n++
			if e.Direction() == domain.DirectionCredit {
				calc += e.Money().Minor()
			} else {
				calc -= e.Money().Minor()
			}
		}
		c, _ := domain.MoneyFromMinor(calc, w.Currency())
		sum = application.LedgerSummary{Stored: w.Balance(), Calculated: c, Entries: n}
	})
	return
}

func (l *lockedReader) ListLedger(ctx context.Context, id uuid.UUID, before *int64, limit int) (out []domain.LedgerEntry, err error) {
	l.with(func(r *repos) {
		for _, e := range r.s.ledger {
			if e.WalletID() == id && (before == nil || e.WalletVersion() < *before) {
				out = append(out, e)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].WalletVersion() > out[j].WalletVersion() })
		if len(out) > limit {
			out = out[:limit]
		}
	})
	return
}

func (l *lockedReader) GetByID(ctx context.Context, id uuid.UUID) (t *domain.WagerTransaction, err error) {
	l.with(func(r *repos) { t, err = r.transactions().GetByID(ctx, id) })
	return
}

func (l *lockedReader) FindByExternal(ctx context.Context, p, e string) (t *domain.WagerTransaction, err error) {
	l.with(func(r *repos) { t, err = r.transactions().FindByExternal(ctx, p, e) })
	return
}

func (l *lockedReader) FindByProviderKeys(ctx context.Context, p, k, e string) (ts []*domain.WagerTransaction, err error) {
	l.with(func(r *repos) { ts, err = r.transactions().FindByProviderKeys(ctx, p, k, e) })
	return
}

func (l *lockedReader) ClaimDue(ctx context.Context, limit int, lease time.Duration) (out []application.DueTransaction, err error) {
	l.with(func(r *repos) {
		now := l.s.Now()
		var ids []uuid.UUID
		for id, t := range r.s.txns {
			if (t.Status == domain.StatusPending || t.Status == domain.StatusPendingReference) && t.NextAttemptAt != nil {
				next := *t.NextAttemptAt
				if lease, ok := r.s.leases[id]; ok && lease.After(next) {
					next = lease
				}
				if !next.After(now) {
					ids = append(ids, id)
				}
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
		for _, id := range ids {
			if len(out) == limit {
				break
			}
			r.s.leases[id] = now.Add(lease)
			out = append(out, application.DueTransaction{ID: id, WalletID: r.s.txns[id].WalletID})
		}
	})
	return
}

type walletRepo struct{ s *state }

func (r *walletRepo) rehydrate(row walletRow) (*domain.Wallet, error) {
	m, err := domain.MoneyFromMinor(row.balance, row.currency)
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(row.id, row.player, m, row.version, row.created, row.updated)
}

func (r *walletRepo) GetForUpdate(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return r.Get(context.Background(), id)
}

func (r *walletRepo) Get(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	row, ok := r.s.wallets[id]
	if !ok {
		return nil, domain.ErrWalletNotFound
	}
	return r.rehydrate(row)
}

func (r *walletRepo) Insert(_ context.Context, w *domain.Wallet) error {
	for _, row := range r.s.wallets {
		if row.player == w.PlayerID() && row.currency == w.Currency() {
			return domain.ErrWalletAlreadyExists
		}
	}
	r.s.wallets[w.ID()] = walletRow{id: w.ID(), player: w.PlayerID(), currency: w.Currency(),
		balance: w.Balance().Minor(), version: w.Version(), created: w.CreatedAt(), updated: w.UpdatedAt()}
	return nil
}

func (r *walletRepo) UpdateBalance(_ context.Context, w *domain.Wallet, expected int64) error {
	row, ok := r.s.wallets[w.ID()]
	if !ok || row.version != expected {
		return application.ErrConcurrentModification
	}
	if w.Balance().IsNegative() {
		panic("memory store: negative balance reached the repository")
	}
	row.balance, row.version, row.updated = w.Balance().Minor(), w.Version(), w.UpdatedAt()
	r.s.wallets[w.ID()] = row
	return nil
}

type txnRepo struct {
	s  *state
	db *Store
}

func (r *txnRepo) load(snap domain.TransactionSnapshot) (*domain.WagerTransaction, error) {
	return domain.RehydrateTransaction(snap)
}

func (r *txnRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	snap, ok := r.s.txns[id]
	if !ok {
		return nil, domain.ErrTransactionNotFound
	}
	return r.load(snap)
}

func (r *txnRepo) GetByIDForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return r.GetByID(ctx, id)
}

func (r *txnRepo) FindByExternal(_ context.Context, provider, external string) (*domain.WagerTransaction, error) {
	for _, snap := range r.s.txns {
		if snap.ProviderID == provider && snap.ExternalTransactionID == external {
			return r.load(snap)
		}
	}
	return nil, domain.ErrTransactionNotFound
}

func (r *txnRepo) FindByProviderKeys(_ context.Context, provider, key, external string) ([]*domain.WagerTransaction, error) {
	var out []*domain.WagerTransaction
	for _, snap := range r.s.txns {
		if snap.ProviderID == provider && (snap.IdempotencyKey == key || snap.ExternalTransactionID == external) {
			t, err := r.load(snap)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
		}
	}
	return out, nil
}

func (r *txnRepo) Insert(_ context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	for _, o := range r.s.txns {
		if s.Origin == domain.OriginExternal && o.ProviderID == s.ProviderID &&
			(o.ExternalTransactionID == s.ExternalTransactionID || o.IdempotencyKey == s.IdempotencyKey) {
			return application.ErrDuplicateTransaction
		}
	}
	r.s.txns[s.ID] = s
	if s.Status == domain.StatusProcessed && s.ReferenceTransactionID != nil && (s.Kind == domain.KindRefund || s.Kind == domain.KindRollback) {
		r.s.reversals[*s.ReferenceTransactionID] = true
	}
	return nil
}

func (r *txnRepo) Update(_ context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	old, ok := r.s.txns[s.ID]
	if !ok || old.Status.IsTerminal() {
		return application.ErrConcurrentModification
	}
	if s.NextAttemptAt != nil {
		next := r.db.Now().Add(s.NextAttemptAt.Sub(s.UpdatedAt))
		s.NextAttemptAt = &next
	}
	r.s.txns[s.ID] = s
	delete(r.s.leases, s.ID)
	if s.Status == domain.StatusProcessed && s.ReferenceTransactionID != nil && (s.Kind == domain.KindRefund || s.Kind == domain.KindRollback) {
		r.s.reversals[*s.ReferenceTransactionID] = true
	}
	return nil
}

func (r *txnRepo) HasSuccessfulReversal(_ context.Context, id uuid.UUID) (bool, error) {
	return r.s.reversals[id], nil
}

func (r *txnRepo) WakeDependents(_ context.Context, provider, external string) error {
	now := r.db.Now()
	for id, snap := range r.s.txns {
		if snap.Status == domain.StatusPendingReference && snap.ProviderID == provider &&
			snap.ReferenceExternalTransactionID == external {
			snap.NextAttemptAt = &now
			r.s.txns[id] = snap
			delete(r.s.leases, id)
		}
	}
	return nil
}

type ledgerRepo struct{ s *state }

func (r *ledgerRepo) Insert(_ context.Context, e domain.LedgerEntry) error {
	for _, o := range r.s.ledger {
		if o.WalletID() == e.WalletID() && (o.TransactionID() == e.TransactionID() || o.WalletVersion() == e.WalletVersion()) {
			return application.ErrConcurrentModification
		}
	}
	r.s.ledger = append(r.s.ledger, e)
	return nil
}

type outboxRepo struct{ s *state }

func (r *outboxRepo) Add(_ context.Context, events ...domain.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return err
		}
		r.s.outbox = append(r.s.outbox, StoredEvent{ID: e.ID(), Type: e.Type(), Aggregate: e.AggregateID(),
			Partition: e.PartitionKey(), Payload: payload})
	}
	return nil
}

type inboxRepo struct{ s *state }

func (r *inboxRepo) Begin(_ context.Context, consumer, id, hash string, _ time.Time) (bool, string, error) {
	k := consumer + "/" + id
	if row, ok := r.s.inbox[k]; ok {
		return false, row.hash, nil
	}
	r.s.inbox[k] = inboxRow{hash: hash}
	return true, hash, nil
}

func (r *inboxRepo) Complete(_ context.Context, consumer, id string, _ time.Time) error {
	k := consumer + "/" + id
	row, ok := r.s.inbox[k]
	if !ok || row.completed {
		return application.ErrConcurrentModification
	}
	row.completed = true
	r.s.inbox[k] = row
	return nil
}
