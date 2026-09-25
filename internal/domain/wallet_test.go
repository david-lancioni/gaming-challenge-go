package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testNow = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func newTestWallet(t testing.TB, balance string) *Wallet {
	t.Helper()
	w, err := NewWallet(NewID(), mustMoney(t, balance, "BRL"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func movement(t testing.TB, amount string) Movement {
	return Movement{TransactionID: NewID(), Amount: mustMoney(t, amount, "BRL"),
		Meta: EventMeta{CorrelationID: "corr", OccurredAt: testNow}}
}

func TestNewWallet(t *testing.T) {
	w := newTestWallet(t, "1000.00")
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" || w.Currency() != "BRL" {
		t.Fatalf("unexpected wallet %+v", w)
	}
	if len(w.PullEvents()) != 0 {
		t.Error("creation must not record events")
	}
	if _, err := NewWallet(uuid.Nil, mustMoney(t, "1.00", "BRL"), testNow); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("nil player: %v", err)
	}
	if _, err := NewWallet(NewID(), Money{}, testNow); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("uninitialized balance: %v", err)
	}
	neg, _ := MoneyFromMinor(-1, "BRL")
	if _, err := NewWallet(NewID(), neg, testNow); !errors.Is(err, ErrNegativeAmount) {
		t.Errorf("negative balance: %v", err)
	}
}

func TestWalletDebitCredit(t *testing.T) {
	w := newTestWallet(t, "100.00")

	e, err := w.Debit(movement(t, "80.00"))
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	if e.Direction() != DirectionDebit || e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" || e.WalletVersion() != 2 {
		t.Fatalf("unexpected entry %+v", e)
	}

	e, err = w.Credit(movement(t, "5.25"))
	if err != nil || w.Balance().Amount() != "25.25" || w.Version() != 3 || e.Direction() != DirectionCredit {
		t.Fatalf("credit: %v %s v%d", err, w.Balance(), w.Version())
	}

	events := w.PullEvents()
	if len(events) != 2 || events[0].Type() != EventWalletBalanceChanged || events[0].Version() != EventVersion {
		t.Fatalf("events = %+v", events)
	}
	if len(w.PullEvents()) != 0 {
		t.Error("PullEvents must clear events")
	}
}

func TestWalletDebitInsufficientFundsLeavesWalletUntouched(t *testing.T) {
	w := newTestWallet(t, "100.00")
	_, err := w.Debit(movement(t, "100.01"))
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("error = %v", err)
	}
	if w.Balance().Amount() != "100.00" || w.Version() != 1 || len(w.PullEvents()) != 0 {
		t.Fatal("failed debit mutated the wallet")
	}
	if _, err := w.Debit(movement(t, "100.00")); err != nil || !w.Balance().IsZero() {
		t.Fatalf("exact debit: %v %s", err, w.Balance())
	}
}

func TestWalletRejectsInvalidMovements(t *testing.T) {
	w := newTestWallet(t, "100.00")
	usd := Movement{TransactionID: NewID(), Amount: mustMoney(t, "1.00", "USD"), Meta: EventMeta{OccurredAt: testNow}}
	if _, err := w.Debit(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("currency mismatch: %v", err)
	}
	zero := Movement{TransactionID: NewID(), Amount: mustMoney(t, "0.00", "BRL"), Meta: EventMeta{OccurredAt: testNow}}
	if _, err := w.Credit(zero); !errors.Is(err, ErrInvalidMoney) {
		t.Errorf("zero movement: %v", err)
	}
	if _, err := w.Credit(Movement{Amount: mustMoney(t, "1.00", "BRL")}); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("missing transaction id: %v", err)
	}
	if _, err := w.Credit(Movement{TransactionID: NewID()}); err == nil {
		t.Error("uninitialized amount must be rejected")
	}
	big, _ := MoneyFromMinor(9223372036854775807, "BRL")
	if _, err := w.Credit(Movement{TransactionID: NewID(), Amount: big, Meta: EventMeta{OccurredAt: testNow}}); !errors.Is(err, ErrMoneyOverflow) {
		t.Errorf("overflow: %v", err)
	}
	if w.Version() != 1 || w.Balance().Amount() != "100.00" {
		t.Error("rejected movements mutated the wallet")
	}
}

func TestRehydrateWalletDoesNotApplyOrEmit(t *testing.T) {
	id, player := NewID(), NewID()
	w, err := RehydrateWallet(id, player, mustMoney(t, "20.00", "BRL"), 7, testNow, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.Balance().Amount() != "20.00" || len(w.PullEvents()) != 0 {
		t.Fatalf("unexpected rehydrated wallet %+v", w)
	}
	neg, _ := MoneyFromMinor(-1, "BRL")
	for name, fn := range map[string]func() error{
		"nil id":     func() error { _, e := RehydrateWallet(uuid.Nil, player, w.Balance(), 1, testNow, testNow); return e },
		"negative":   func() error { _, e := RehydrateWallet(id, player, neg, 1, testNow, testNow); return e },
		"version 0":  func() error { _, e := RehydrateWallet(id, player, w.Balance(), 0, testNow, testNow); return e },
		"no balance": func() error { _, e := RehydrateWallet(id, player, Money{}, 1, testNow, testNow); return e },
	} {
		if err := fn(); !errors.Is(err, ErrInvalidWallet) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestLedgerEntryArithmetic(t *testing.T) {
	wid, tid := NewID(), NewID()
	m10, m3, m13 := mustMoney(t, "10.00", "BRL"), mustMoney(t, "3.00", "BRL"), mustMoney(t, "13.00", "BRL")
	if _, err := NewLedgerEntry(wid, tid, DirectionCredit, m3, m10, m13, 2, testNow); err != nil {
		t.Errorf("valid credit: %v", err)
	}
	if _, err := NewLedgerEntry(wid, tid, DirectionDebit, m3, m13, m10, 2, testNow); err != nil {
		t.Errorf("valid debit: %v", err)
	}
	bad := []struct {
		name string
		dir  Direction
		mv   Money
		b, a Money
		ver  int64
	}{
		{"credit wrong sum", DirectionCredit, m3, m10, m10, 2},
		{"debit wrong diff", DirectionDebit, m3, m13, m13, 2},
		{"direction swapped", DirectionDebit, m3, m10, m13, 2},
		{"unknown direction", "SIDEWAYS", m3, m10, m13, 2},
		{"zero movement", DirectionCredit, mustMoney(t, "0.00", "BRL"), m10, m10, 2},
		{"version 0", DirectionCredit, m3, m10, m13, 0},
		{"uninitialized", DirectionCredit, m3, Money{}, m13, 2},
	}
	for _, tc := range bad {
		if _, err := NewLedgerEntry(wid, tid, tc.dir, tc.mv, tc.b, tc.a, tc.ver, testNow); !errors.Is(err, ErrInvalidLedger) {
			t.Errorf("%s: error = %v", tc.name, err)
		}
	}
}

func TestOpenWalletWithPositiveBalance(t *testing.T) {
	player := NewID()
	op, err := OpenWallet(player, mustMoney(t, "1000.00", "BRL"), "corr-1", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if op.Wallet.Version() != 1 || op.Wallet.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet = %+v", op.Wallet)
	}
	tx := op.Transaction
	if tx == nil || tx.Kind() != KindOpening || tx.Status() != StatusProcessed || tx.Origin() != OriginInternal {
		t.Fatalf("transaction = %+v", tx)
	}
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" || tx.PayloadHash() != "" ||
		tx.RoundID() != "" || tx.GameID() != "" || tx.ReferenceExternalID() != "" {
		t.Error("external metadata must not apply to OPENING")
	}
	e := op.Entry
	if e == nil || e.Direction() != DirectionCredit || e.BalanceBefore().Amount() != "0.00" || e.BalanceAfter().Amount() != "1000.00" ||
		e.WalletVersion() != 1 || e.TransactionID() != tx.ID() {
		t.Fatalf("entry = %+v", e)
	}
	if len(op.Events) != 2 || op.Events[0].Type() != EventWagerTransactionProcessed || op.Events[1].Type() != EventWalletBalanceChanged {
		t.Fatalf("events = %+v", op.Events)
	}
	ev := op.Events[1].Data().(WalletBalanceChangedData)
	if ev.WalletVersion != 1 || ev.Direction != DirectionCredit || ev.BalanceBefore.Amount() != "0.00" {
		t.Errorf("balance event = %+v", ev)
	}
}

func TestOpenWalletWithZeroBalanceCreatesNoFinancialRecords(t *testing.T) {
	op, err := OpenWallet(NewID(), mustMoney(t, "0.00", "BRL"), "corr", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if op.Transaction != nil || op.Entry != nil || len(op.Events) != 0 || op.Wallet.Version() != 1 {
		t.Fatalf("zero opening produced financial records: %+v", op)
	}
}
