package application_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
	"github.com/dlancioni/backend-challenge-go/internal/testutil/memory"
)

var epoch = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type env struct {
	t        *testing.T
	store    *memory.Store
	proc     *application.Processor
	wagering *application.WageringService
	wallets  *application.WalletService
	resolver *application.PendingResolver
	now      time.Time
	mu       sync.Mutex
}

func (e *env) clock() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *env) advance(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.now = e.now.Add(d)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, store: memory.NewStore(), now: epoch}
	e.store.Now = e.clock
	policy := application.PendingPolicy{TTL: 10 * time.Minute, MaxAttempts: 4, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.proc = application.NewProcessor(e.clock, policy, nil)
	e.wagering = application.NewWageringService(e.store, e.store.TransactionReader(), e.proc, nil, log)
	e.wallets = application.NewWalletService(e.store, e.store.WalletReader(), e.clock, nil, log)
	e.resolver = application.NewPendingResolver(e.store, e.store.PendingQueue(), e.proc, e.clock, 30*time.Second, nil, log)
	return e
}

func money(t testing.TB, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (e *env) openWallet(amount string) *domain.Wallet {
	e.t.Helper()
	w, err := e.wallets.Open(context.Background(), uuid.NewString(), money(e.t, amount), "corr")
	if err != nil {
		e.t.Fatal(err)
	}
	return w
}

func (e *env) params(w *domain.Wallet, kind domain.TransactionKind, amount string) domain.ExternalParams {
	ext := "ext-" + uuid.NewString()
	return domain.ExternalParams{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "key-" + ext,
		PlayerID: w.PlayerID().String(), WalletID: w.ID().String(), RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: money(e.t, amount), CorrelationID: "corr",
	}
}

func (e *env) submit(p domain.ExternalParams) application.Outcome {
	e.t.Helper()
	out, err := e.wagering.Submit(context.Background(), application.SourceHTTP, p)
	if err != nil {
		e.t.Fatalf("submit %s: %v", p.Kind, err)
	}
	return out
}

func (e *env) balance(w *domain.Wallet) string {
	e.t.Helper()
	got, err := e.store.Wallet(w.ID())
	if err != nil {
		e.t.Fatal(err)
	}
	return got.Balance().Amount()
}

func (e *env) reversal(w *domain.Wallet, kind domain.TransactionKind, amount string, ref domain.ExternalParams) domain.ExternalParams {
	p := e.params(w, kind, amount)
	p.ReferenceExternalTransactionID = ref.ExternalTransactionID
	return p
}

func TestHappyPathsAndBalances(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")

	bet := e.params(w, domain.KindBet, "30.00")
	out := e.submit(bet)
	if out.Transaction.Status() != domain.StatusProcessed || out.Replay || out.Transaction.ResultBalance().Amount() != "70.00" {
		t.Fatalf("bet = %+v", out.Transaction.Snapshot())
	}
	win := e.params(w, domain.KindWin, "50.00")
	win.ReferenceExternalTransactionID = bet.ExternalTransactionID
	if got := e.submit(win).Transaction.ResultBalance().Amount(); got != "120.00" {
		t.Fatalf("win balance = %s", got)
	}
	loss := e.submit(e.params(w, domain.KindLoss, "0.00"))
	if loss.Transaction.ResultBalance().Amount() != "120.00" {
		t.Fatalf("loss = %+v", loss.Transaction.Snapshot())
	}
	entries := len(e.store.Ledger())
	if entries != 3 {
		t.Fatalf("ledger entries = %d", entries)
	}
	if got, _ := e.store.Wallet(w.ID()); got.Version() != 3 {
		t.Fatalf("version = %d (LOSS must not bump it)", got.Version())
	}
}

func TestRejectionsAreAuditedWithoutMovement(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("50.00")
	before := len(e.store.Ledger())

	out := e.submit(e.params(w, domain.KindBet, "80.00"))
	if out.Transaction.Status() != domain.StatusRejected || out.Transaction.FailureCode() != domain.FailureInsufficientFunds {
		t.Fatalf("bet = %+v", out.Transaction.Snapshot())
	}
	if len(e.store.Ledger()) != before || e.balance(w) != "50.00" {
		t.Fatal("a rejection must not move money")
	}
	var rejected int
	for _, ev := range e.store.Events() {
		if ev.Type == domain.EventWagerTransactionRejected {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("rejection events = %d", rejected)
	}
	p := e.params(w, domain.KindBet, "1.00")
	p.PlayerID = uuid.NewString()
	if got := e.submit(p).Transaction.FailureCode(); got != domain.FailureWalletPlayerMismatch {
		t.Fatalf("code = %s", got)
	}
	p = e.params(w, domain.KindBet, "1.00")
	p.WalletID = uuid.NewString()
	if _, err := e.wagering.Submit(context.Background(), application.SourceHTTP, p); !errors.Is(err, domain.ErrWalletNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestIdempotencyAndConflicts(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	bet := e.params(w, domain.KindBet, "10.00")
	first := e.submit(bet)
	e.submit(e.params(w, domain.KindWin, "5.00"))

	replay := e.submit(bet)
	if !replay.Replay || replay.Transaction.ID() != first.Transaction.ID() || replay.Transaction.ResultBalance().Amount() != "90.00" {
		t.Fatalf("replay = %+v", replay)
	}
	changed := bet
	changed.Money = money(t, "11.00")
	if _, err := e.wagering.Submit(context.Background(), application.SourceHTTP, changed); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("payload conflict: %v", err)
	}
	otherKey := bet
	otherKey.IdempotencyKey = "another"
	if _, err := e.wagering.Submit(context.Background(), application.SourceHTTP, otherKey); !errors.Is(err, domain.ErrExternalTransactionConflict) {
		t.Fatalf("external conflict: %v", err)
	}
	if got := e.balance(w); got != "95.00" {
		t.Fatalf("balance = %s", got)
	}
	bad := bet
	bad.Kind = domain.KindOpening
	if _, err := e.wagering.Submit(context.Background(), application.SourceHTTP, bad); !errors.Is(err, domain.ErrKindNotAllowed) {
		t.Fatalf("OPENING: %v", err)
	}
}

func TestReversalRules(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	code := func(o application.Outcome) domain.FailureCode { return o.Transaction.FailureCode() }

	bet := e.params(w, domain.KindBet, "25.00")
	e.submit(bet)
	refund := e.reversal(w, domain.KindRefund, "25.00", bet)
	if got := e.submit(refund).Transaction.ResultBalance().Amount(); got != "100.00" {
		t.Fatalf("refund balance = %s", got)
	}
	if c := code(e.submit(e.reversal(w, domain.KindRefund, "25.00", bet))); c != domain.FailureReferenceAlreadyReversed {
		t.Fatalf("second refund = %s", c)
	}
	if c := code(e.submit(e.reversal(w, domain.KindRollback, "25.00", bet))); c != domain.FailureReferenceAlreadyReversed {
		t.Fatalf("rollback after refund = %s", c)
	}
	rbRefund := e.reversal(w, domain.KindRollback, "25.00", refund)
	if got := e.submit(rbRefund).Transaction.ResultBalance().Amount(); got != "75.00" {
		t.Fatalf("rollback of refund = %s", got)
	}
	if c := code(e.submit(e.reversal(w, domain.KindRollback, "25.00", rbRefund))); c != domain.FailureReferenceKindInvalid {
		t.Fatalf("rollback of rollback = %s", c)
	}
	win := e.params(w, domain.KindWin, "40.00")
	e.submit(win)
	e.submit(e.params(w, domain.KindBet, "115.00"))
	if c := code(e.submit(e.reversal(w, domain.KindRollback, "40.00", win))); c != domain.FailureReversalInsufficientFunds {
		t.Fatalf("rollback of win without funds = %s", c)
	}
	bet2 := e.params(w, domain.KindWin, "10.00")
	e.submit(bet2)
	if c := code(e.submit(e.reversal(w, domain.KindRollback, "9.99", bet2))); c != domain.FailureReferenceAmountMismatch {
		t.Fatalf("partial rollback = %s", c)
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	bet := e.params(w, domain.KindBet, "40.00")
	refund := e.reversal(w, domain.KindRefund, "40.00", bet)

	out := e.submit(refund)
	if out.Transaction.Status() != domain.StatusPendingReference || out.Transaction.ExpiresAt() == nil {
		t.Fatalf("refund = %+v", out.Transaction.Snapshot())
	}
	if e.balance(w) != "100.00" {
		t.Fatal("a pending reversal must not move money")
	}
	if n, _ := e.resolver.RunOnce(context.Background(), 10); n != 0 {
		t.Fatalf("claimed %d before it is due", n)
	}
	e.advance(2 * time.Second)
	if n, _ := e.resolver.RunOnce(context.Background(), 10); n != 1 {
		t.Fatalf("claimed %d, want 1", n)
	}
	tx, _ := e.store.Transaction("provider-a", refund.ExternalTransactionID)
	if tx.Status() != domain.StatusPendingReference || tx.Attempts() != 1 {
		t.Fatalf("after one failed attempt: %+v", tx.Snapshot())
	}

	e.submit(bet)
	if n, _ := e.resolver.RunOnce(context.Background(), 10); n != 1 {
		t.Fatalf("claimed %d after the reference arrived", n)
	}
	tx, _ = e.store.Transaction("provider-a", refund.ExternalTransactionID)
	if tx.Status() != domain.StatusProcessed || tx.ReferenceTransactionID() == nil || e.balance(w) != "100.00" {
		t.Fatalf("refund = %+v balance=%s", tx.Snapshot(), e.balance(w))
	}
	var pending, processed int
	for _, ev := range e.store.Events() {
		switch ev.Type {
		case domain.EventWagerTransactionPendingReference:
			pending++
		case domain.EventWagerTransactionProcessed:
			processed++
		}
	}
	if pending != 1 || processed != 3 {
		t.Fatalf("events: pending=%d processed=%d", pending, processed)
	}
}

func TestPendingReferenceExhaustion(t *testing.T) {
	t.Run("attempt budget", func(t *testing.T) {
		e := newEnv(t)
		w := e.openWallet("10.00")
		p := e.params(w, domain.KindRollback, "1.00")
		p.ReferenceExternalTransactionID = "never"
		e.submit(p)
		for i := 0; i < 10; i++ {
			e.advance(15 * time.Second)
			if _, err := e.resolver.RunOnce(context.Background(), 10); err != nil {
				t.Fatal(err)
			}
		}
		tx, _ := e.store.Transaction("provider-a", p.ExternalTransactionID)
		if tx.Status() != domain.StatusRejected || tx.FailureCode() != domain.FailureReferenceNotFound {
			t.Fatalf("tx = %+v", tx.Snapshot())
		}
	})
	t.Run("ttl", func(t *testing.T) {
		e := newEnv(t)
		w := e.openWallet("10.00")
		p := e.params(w, domain.KindRefund, "1.00")
		p.ReferenceExternalTransactionID = "never"
		e.submit(p)
		e.advance(11 * time.Minute)
		if _, err := e.resolver.RunOnce(context.Background(), 10); err != nil {
			t.Fatal(err)
		}
		tx, _ := e.store.Transaction("provider-a", p.ExternalTransactionID)
		if tx.Status() != domain.StatusRejected || tx.FailureCode() != domain.FailureReferenceNotFound {
			t.Fatalf("tx = %+v", tx.Snapshot())
		}
	})
	t.Run("reference exists but is still pending", func(t *testing.T) {
		e := newEnv(t)
		w := e.openWallet("10.00")
		inner := e.params(w, domain.KindRefund, "1.00")
		inner.ReferenceExternalTransactionID = "never"
		e.submit(inner)
		outer := e.reversal(w, domain.KindRollback, "1.00", inner)
		e.submit(outer)
		e.advance(11 * time.Minute)
		if _, err := e.resolver.RunOnce(context.Background(), 10); err != nil {
			t.Fatal(err)
		}
		in, _ := e.store.Transaction("provider-a", inner.ExternalTransactionID)
		out, _ := e.store.Transaction("provider-a", outer.ExternalTransactionID)
		if in.Status() != domain.StatusRejected {
			t.Fatalf("inner = %+v", in.Snapshot())
		}
		if out.Status() != domain.StatusRejected ||
			(out.FailureCode() != domain.FailureReferenceUnavailable && out.FailureCode() != domain.FailureReferenceNotProcessed) {
			t.Fatalf("outer = %+v", out.Snapshot())
		}
	})
}

func TestPermanentFailureOfPendingOperationIsRecorded(t *testing.T) {
	e := newEnv(t)
	max, _ := domain.MoneyFromMinor(9223372036854775807, "BRL")
	w, err := e.wallets.Open(context.Background(), uuid.NewString(), max, "c")
	if err != nil {
		t.Fatal(err)
	}
	p := e.params(w, domain.KindWin, "0.01")
	txn, err := domain.NewExternalTransaction(p, e.clock())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.store.Do(context.Background(), func(ctx context.Context, r application.Repositories) error {
		return r.Transactions().Insert(ctx, txn)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.resolver.RunOnce(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	got, _ := e.store.Transaction("provider-a", p.ExternalTransactionID)
	if got.Status() != domain.StatusFailed || got.FailureCode() != domain.FailureProcessingError {
		t.Fatalf("tx = %+v", got.Snapshot())
	}
	if e.balance(w) != max.Amount() {
		t.Fatal("balance changed")
	}
	e.advance(time.Hour)
	if n, _ := e.resolver.RunOnce(context.Background(), 10); n != 0 {
		t.Fatalf("claimed %d terminal transactions", n)
	}
}

func TestParallelDuplicatesProduceASingleDebit(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("1000.00")
	bet := e.params(w, domain.KindBet, "25.00")

	var wg sync.WaitGroup
	results := make([]application.Outcome, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, err := e.wagering.Submit(context.Background(), application.SourceHTTP, bet)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = out
		}(i)
	}
	wg.Wait()
	fresh := 0
	for _, r := range results {
		if r.Transaction == nil || r.Transaction.Status() != domain.StatusProcessed {
			t.Fatalf("outcome = %+v", r)
		}
		if !r.Replay {
			fresh++
		}
	}
	if fresh != 1 || e.balance(w) != "975.00" || len(e.store.Ledger()) != 2 {
		t.Fatalf("fresh=%d balance=%s ledger=%d", fresh, e.balance(w), len(e.store.Ledger()))
	}
}

func TestTwoBetsOf80OnABalanceOf100(t *testing.T) {
	for round := 0; round < 20; round++ {
		e := newEnv(t)
		w := e.openWallet("100.00")
		outs := make([]application.Outcome, 2)
		var wg sync.WaitGroup
		for i := range outs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				outs[i] = e.submit(e.params(w, domain.KindBet, "80.00"))
			}(i)
		}
		wg.Wait()
		processed, rejected := 0, 0
		for _, o := range outs {
			switch o.Transaction.Status() {
			case domain.StatusProcessed:
				processed++
			case domain.StatusRejected:
				rejected++
			}
		}
		if processed != 1 || rejected != 1 || e.balance(w) != "20.00" {
			t.Fatalf("processed=%d rejected=%d balance=%s", processed, rejected, e.balance(w))
		}
	}
}

func TestWalletOpeningEvents(t *testing.T) {
	e := newEnv(t)
	e.openWallet("1000.00")
	var types []domain.EventType
	for _, ev := range e.store.Events() {
		types = append(types, ev.Type)
	}
	if len(types) != 2 || types[0] != domain.EventWagerTransactionProcessed || types[1] != domain.EventWalletBalanceChanged {
		t.Fatalf("events = %v", types)
	}
	before := len(e.store.Events())
	e.openWallet("0.00")
	if len(e.store.Events()) != before || len(e.store.Ledger()) != 1 {
		t.Fatal("a zero opening must create no ledger entry or event")
	}
	w := e.openWallet("5.00")
	if _, err := e.wallets.Open(context.Background(), w.PlayerID().String(), money(t, "1.00"), "c"); !errors.Is(err, domain.ErrWalletAlreadyExists) {
		t.Fatalf("duplicate wallet: %v", err)
	}
	if _, err := e.wallets.Open(context.Background(), "bad", money(t, "1.00"), "c"); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("invalid player: %v", err)
	}
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	e := newEnv(t)
	w := e.openWallet("100.00")
	for i := 0; i < 5; i++ {
		e.submit(e.params(w, domain.KindBet, "1.00"))
	}
	var versions []int64
	cursor := ""
	for {
		page, err := e.wallets.Ledger(context.Background(), w.ID(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, en := range page.Entries {
			versions = append(versions, en.WalletVersion())
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	want := []int64{6, 5, 4, 3, 2, 1}
	if len(versions) != len(want) {
		t.Fatalf("versions = %v", versions)
	}
	for i := range want {
		if versions[i] != want[i] {
			t.Fatalf("versions = %v", versions)
		}
	}
	for _, bad := range []struct {
		cursor string
		limit  int
	}{{"garbage", 10}, {"", -1}, {"", 201}} {
		if _, err := e.wallets.Ledger(context.Background(), w.ID(), bad.cursor, bad.limit); err == nil {
			t.Errorf("cursor=%q limit=%d must be rejected", bad.cursor, bad.limit)
		}
	}
	if _, err := e.wallets.Ledger(context.Background(), uuid.New(), "", 10); !errors.Is(err, domain.ErrWalletNotFound) {
		t.Fatalf("unknown wallet: %v", err)
	}

	res, err := e.wallets.Reconcile(context.Background(), w.ID())
	if err != nil || !res.Consistent || res.CheckedEntries != 6 || res.Difference.Amount() != "0.00" {
		t.Fatalf("reconciliation = %+v %v", res, err)
	}
	e.store.Corrupt(w.ID(), 9500+7)
	res, _ = e.wallets.Reconcile(context.Background(), w.ID())
	if res.Consistent || res.Difference.Amount() != "0.07" || res.CalculatedBalance.Amount() != "95.00" {
		t.Fatalf("divergence = %+v", res)
	}
	if got, _ := e.store.Wallet(w.ID()); got.Balance().Minor() != 9507 {
		t.Fatal("reconciliation must not change the balance")
	}
}

func TestBackoff(t *testing.T) {
	if application.Backoff(0, time.Minute, 3) != 0 {
		t.Error("zero base means no delay")
	}
	for attempt := 0; attempt < 12; attempt++ {
		d := application.Backoff(time.Second, 30*time.Second, attempt)
		nominal := time.Second << attempt
		if nominal > 30*time.Second || nominal <= 0 {
			nominal = 30 * time.Second
		}
		lo, hi := time.Duration(float64(nominal)*0.79), time.Duration(float64(nominal)*1.21)
		if d < lo || d > hi {
			t.Errorf("attempt %d: %s outside [%s, %s]", attempt, d, lo, hi)
		}
	}
	if application.Backoff(time.Second, time.Minute, -5) <= 0 {
		t.Error("negative attempts behave like zero")
	}
}
