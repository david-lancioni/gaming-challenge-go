package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func baseParams(t testing.TB, kind TransactionKind, amount string) ExternalParams {
	p := ExternalParams{
		ProviderID: "provider-a", ExternalTransactionID: "tx-1", IdempotencyKey: "provider-a:tx-1",
		PlayerID: NewID().String(), WalletID: NewID().String(), RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: mustMoney(t, amount, "BRL"), CorrelationID: "corr", CausationID: "msg-1",
	}
	if kind.IsReversal() {
		p.ReferenceExternalTransactionID = "tx-0"
	}
	return p
}

func TestNewExternalTransactionKindAndAmountPolicy(t *testing.T) {
	valid := []struct {
		kind   TransactionKind
		amount string
	}{
		{KindBet, "25.00"}, {KindWin, "50.00"}, {KindLoss, "0.00"}, {KindRefund, "25.00"}, {KindRollback, "25.00"},
	}
	for _, tc := range valid {
		tx, err := NewExternalTransaction(baseParams(t, tc.kind, tc.amount), testNow)
		if err != nil {
			t.Errorf("%s %s: %v", tc.kind, tc.amount, err)
			continue
		}
		if tx.Status() != StatusPending || tx.Origin() != OriginExternal || len(tx.PullEvents()) != 0 {
			t.Errorf("%s: unexpected initial state", tc.kind)
		}
	}

	invalidCases := []struct {
		name   string
		kind   TransactionKind
		amount string
	}{
		{"BET zero", KindBet, "0.00"},
		{"WIN zero", KindWin, "0.00"},
		{"REFUND zero", KindRefund, "0.00"},
		{"ROLLBACK zero", KindRollback, "0.00"},
		{"LOSS positive", KindLoss, "0.01"},
	}
	for _, tc := range invalidCases {
		if _, err := NewExternalTransaction(baseParams(t, tc.kind, tc.amount), testNow); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: error = %v", tc.name, err)
		}
	}
}

func TestNewExternalTransactionRejectsOpeningAndUnknownKinds(t *testing.T) {
	for _, k := range []TransactionKind{KindOpening, "", "TRANSFER", "bet"} {
		p := baseParams(t, KindBet, "1.00")
		p.Kind = k
		_, err := NewExternalTransaction(p, testNow)
		if !errors.Is(err, ErrKindNotAllowed) || !errors.Is(err, ErrInvalidInput) {
			t.Errorf("kind %q: error = %v", k, err)
		}
	}
}

func TestNewExternalTransactionFieldValidation(t *testing.T) {
	mutations := map[string]func(*ExternalParams){
		"provider empty":        func(p *ExternalParams) { p.ProviderID = "" },
		"provider padded":       func(p *ExternalParams) { p.ProviderID = " provider-a" },
		"provider too long":     func(p *ExternalParams) { p.ProviderID = strings.Repeat("a", 129) },
		"provider control char": func(p *ExternalParams) { p.ProviderID = "a\nb" },
		"external id empty":     func(p *ExternalParams) { p.ExternalTransactionID = "" },
		"key empty":             func(p *ExternalParams) { p.IdempotencyKey = "" },
		"key too long":          func(p *ExternalParams) { p.IdempotencyKey = strings.Repeat("k", 256) },
		"round empty":           func(p *ExternalParams) { p.RoundID = "" },
		"game empty":            func(p *ExternalParams) { p.GameID = "" },
		"player not uuid":       func(p *ExternalParams) { p.PlayerID = "player" },
		"player nil uuid":       func(p *ExternalParams) { p.PlayerID = "00000000-0000-0000-0000-000000000000" },
		"player uppercase-less": func(p *ExternalParams) { p.PlayerID = "{" + NewID().String() + "}" },
		"wallet not uuid":       func(p *ExternalParams) { p.WalletID = "" },
		"money missing":         func(p *ExternalParams) { p.Money = Money{} },
		"invalid utf8":          func(p *ExternalParams) { p.RoundID = "\xff\xfe" },
		"BET with reference":    func(p *ExternalParams) { p.ReferenceExternalTransactionID = "tx-0" },
		"self reference":        func(p *ExternalParams) { p.Kind = KindWin; p.ReferenceExternalTransactionID = p.ExternalTransactionID },
	}
	for name, mutate := range mutations {
		p := baseParams(t, KindBet, "1.00")
		mutate(&p)
		if _, err := NewExternalTransaction(p, testNow); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: error = %v", name, err)
		}
	}
	for _, kind := range []TransactionKind{KindRefund, KindRollback} {
		p := baseParams(t, kind, "1.00")
		p.ReferenceExternalTransactionID = ""
		if _, err := NewExternalTransaction(p, testNow); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s without reference: %v", kind, err)
		}
	}
	p := baseParams(t, KindWin, "1.00")
	if _, err := NewExternalTransaction(p, testNow); err != nil {
		t.Errorf("WIN without reference: %v", err)
	}
	p.ReferenceExternalTransactionID = "bet-1"
	if _, err := NewExternalTransaction(p, testNow); err != nil {
		t.Errorf("WIN with reference: %v", err)
	}
	p = baseParams(t, KindLoss, "0.00")
	p.ReferenceExternalTransactionID = "bet-1"
	if _, err := NewExternalTransaction(p, testNow); !errors.Is(err, ErrInvalidInput) {
		t.Errorf("LOSS with reference: %v", err)
	}
}

func TestPayloadHashIsDeterministicAndScopedToBusinessFields(t *testing.T) {
	p := baseParams(t, KindBet, "25.00")
	a, _ := NewExternalTransaction(p, testNow)
	q := p
	q.IdempotencyKey = "another-key"
	q.CorrelationID, q.CausationID = "other", "other"
	b, _ := NewExternalTransaction(q, testNow.Add(time.Hour))
	if a.PayloadHash() != b.PayloadHash() {
		t.Fatal("hash must ignore idempotency key, correlation and timestamps")
	}
	if len(a.PayloadHash()) != 64 {
		t.Fatalf("hash = %q", a.PayloadHash())
	}
	changes := map[string]func(*ExternalParams){
		"amount":    func(p *ExternalParams) { p.Money = mustMoney(t, "25.01", "BRL") },
		"currency":  func(p *ExternalParams) { p.Money = mustMoney(t, "25.00", "USD") },
		"round":     func(p *ExternalParams) { p.RoundID = "round-2" },
		"game":      func(p *ExternalParams) { p.GameID = "game-2" },
		"kind":      func(p *ExternalParams) { p.Kind = KindWin },
		"wallet":    func(p *ExternalParams) { p.WalletID = NewID().String() },
		"player":    func(p *ExternalParams) { p.PlayerID = NewID().String() },
		"external":  func(p *ExternalParams) { p.ExternalTransactionID = "tx-2" },
		"provider":  func(p *ExternalParams) { p.ProviderID = "provider-b" },
		"reference": func(p *ExternalParams) { p.Kind = KindWin; p.ReferenceExternalTransactionID = "x" },
	}
	for name, mutate := range changes {
		c := p
		mutate(&c)
		d, err := NewExternalTransaction(c, testNow)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if d.PayloadHash() == a.PayloadHash() {
			t.Errorf("changing %s must change the hash", name)
		}
	}
}

func TestPayloadHashCanonicalJSONGolden(t *testing.T) {
	fields := PayloadFields{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "round-987", GameID: "fortune-chimp", Kind: KindBet, Money: mustMoney(t, "25.00", "BRL"),
	}
	const wantCanonical = `{"amount":"25.00","currency":"BRL","externalTransactionId":"transaction-123",` +
		`"gameId":"fortune-chimp","kind":"BET","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",` +
		`"providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	if c := string(CanonicalPayload(fields)); c != wantCanonical {
		t.Fatalf("canonical payload =\n%s\nwant\n%s", c, wantCanonical)
	}
	sum := sha256.Sum256([]byte(wantCanonical))
	got := ComputePayloadHash(fields)
	if got != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash %s is not sha256 of the canonical payload", got)
	}
	again := ComputePayloadHash(PayloadFields{
		GameID: "fortune-chimp", Kind: KindBet, RoundID: "round-987", ProviderID: "provider-a",
		WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37", PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		ExternalTransactionID: "transaction-123", Money: mustMoney(t, "25.00", "BRL"),
	})
	if got != again {
		t.Fatal("field order must not matter")
	}
}

func newPending(t testing.TB) *WagerTransaction {
	tx, err := NewExternalTransaction(baseParams(t, KindRefund, "10.00"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTransactionStateMachine(t *testing.T) {
	later := testNow.Add(time.Minute)
	bal := mustMoney(t, "10.00", "BRL")

	t.Run("pending to processed", func(t *testing.T) {
		tx := newPending(t)
		if err := tx.MarkProcessed(later, bal, nil); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != StatusProcessed || tx.ResultBalance() == nil || tx.NextAttemptAt() != nil || tx.ProcessedAt() == nil {
			t.Fatalf("state = %+v", tx.Snapshot())
		}
		ev := tx.PullEvents()
		if len(ev) != 1 || ev[0].Type() != EventWagerTransactionProcessed {
			t.Fatalf("events = %+v", ev)
		}
	})

	t.Run("pending to reference to processed", func(t *testing.T) {
		tx := newPending(t)
		if err := tx.WaitForReference(later, later.Add(time.Second), later.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != StatusPendingReference || tx.ExpiresAt() == nil {
			t.Fatal("expected PENDING_REFERENCE")
		}
		if ev := tx.PullEvents(); len(ev) != 1 || ev[0].Type() != EventWagerTransactionPendingReference {
			t.Fatalf("events = %+v", ev)
		}
		if err := tx.RetryReference(later, later.Add(2*time.Second)); err != nil || tx.Attempts() != 1 {
			t.Fatalf("retry: %v attempts=%d", err, tx.Attempts())
		}
		if len(tx.PullEvents()) != 0 {
			t.Error("retry must not emit events")
		}
		if err := tx.MarkProcessed(later, bal, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reject", func(t *testing.T) {
		tx := newPending(t)
		if err := tx.Reject(later, FailureReferenceNotFound, "gone", nil); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != StatusRejected || tx.FailureCode() != FailureReferenceNotFound {
			t.Fatal("expected REJECTED")
		}
		if ev := tx.PullEvents(); len(ev) != 1 || ev[0].Type() != EventWagerTransactionRejected {
			t.Fatalf("events = %+v", ev)
		}
		if err := newPending(t).Reject(later, "", "x", nil); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("empty failure code: %v", err)
		}
	})

	t.Run("fail emits no event", func(t *testing.T) {
		tx := newPending(t)
		if err := tx.Fail(later, FailureProcessingError, "boom"); err != nil || tx.Status() != StatusFailed {
			t.Fatalf("fail: %v", err)
		}
		if len(tx.PullEvents()) != 0 {
			t.Error("FAILED must not emit integration events")
		}
	})

	t.Run("terminal states are final", func(t *testing.T) {
		finals := map[string]func(*WagerTransaction) error{
			"processed": func(tx *WagerTransaction) error { return tx.MarkProcessed(later, bal, nil) },
			"rejected":  func(tx *WagerTransaction) error { return tx.Reject(later, FailureInsufficientFunds, "", nil) },
			"failed":    func(tx *WagerTransaction) error { return tx.Fail(later, FailureProcessingError, "") },
		}
		for name, toFinal := range finals {
			tx := newPending(t)
			if err := toFinal(tx); err != nil {
				t.Fatal(err)
			}
			tx.PullEvents()
			attempts := map[string]error{
				"processed": tx.MarkProcessed(later, bal, nil),
				"rejected":  tx.Reject(later, FailureInsufficientFunds, "", nil),
				"failed":    tx.Fail(later, FailureProcessingError, ""),
				"wait":      tx.WaitForReference(later, later, later),
				"retry":     tx.RetryReference(later, later),
			}
			for to, err := range attempts {
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("%s -> %s: error = %v", name, to, err)
				}
			}
			if len(tx.PullEvents()) != 0 {
				t.Errorf("%s: rejected transitions emitted events", name)
			}
		}
	})

	t.Run("invalid transitions", func(t *testing.T) {
		tx := newPending(t)
		if err := tx.RetryReference(later, later); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("retry from PENDING: %v", err)
		}
		_ = tx.WaitForReference(later, later, later)
		if err := tx.WaitForReference(later, later, later); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("wait twice: %v", err)
		}
		usd := mustMoney(t, "10.00", "USD")
		if err := newPending(t).MarkProcessed(later, usd, nil); !errors.Is(err, ErrInvalidMoney) {
			t.Errorf("wrong currency balance: %v", err)
		}
		bet, _ := NewExternalTransaction(baseParams(t, KindBet, "1.00"), testNow)
		if err := bet.WaitForReference(later, later, later); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("BET cannot wait for a reference: %v", err)
		}
	})
}

func TestRehydrateTransactionDoesNotTransitionOrEmit(t *testing.T) {
	tx := newPending(t)
	_ = tx.MarkProcessed(testNow, mustMoney(t, "10.00", "BRL"), nil)
	tx.PullEvents()
	snap := tx.Snapshot()

	back, err := RehydrateTransaction(snap)
	if err != nil {
		t.Fatal(err)
	}
	if back.Status() != StatusProcessed || len(back.PullEvents()) != 0 {
		t.Fatal("rehydration must preserve state and emit nothing")
	}
	if err := back.Fail(testNow, FailureProcessingError, ""); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("rehydrated terminal transaction accepted a transition: %v", err)
	}

	broken := map[string]func(*TransactionSnapshot){
		"unknown status":      func(s *TransactionSnapshot) { s.Status = "WHATEVER" },
		"processed no result": func(s *TransactionSnapshot) { s.ResultBalance = nil },
		"rejected no code":    func(s *TransactionSnapshot) { s.Status = StatusRejected; s.FailureCode = "" },
		"pending ref no ttl":  func(s *TransactionSnapshot) { s.Status = StatusPendingReference; s.ExpiresAt = nil },
		"external no key":     func(s *TransactionSnapshot) { s.IdempotencyKey = "" },
		"opening external":    func(s *TransactionSnapshot) { s.Kind = KindOpening },
		"internal with key":   func(s *TransactionSnapshot) { s.Origin = OriginInternal; s.Kind = KindOpening },
		"no money":            func(s *TransactionSnapshot) { s.Money = Money{} },
	}
	for name, mutate := range broken {
		s := snap
		mutate(&s)
		if _, err := RehydrateTransaction(s); !errors.Is(err, ErrInvalidState) {
			t.Errorf("%s: error = %v", name, err)
		}
	}
}

func TestReferenceRules(t *testing.T) {
	build := func(kind TransactionKind, amount string, mut func(*ExternalParams)) *WagerTransaction {
		p := baseParams(t, kind, amount)
		if mut != nil {
			mut(&p)
		}
		tx, err := NewExternalTransaction(p, testNow)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	wallet, player := NewID().String(), NewID().String()
	same := func(p *ExternalParams) { p.WalletID, p.PlayerID = wallet, player }

	bet := build(KindBet, "10.00", same)
	win := build(KindWin, "10.00", same)
	refund := build(KindRefund, "10.00", same)
	rollback := build(KindRollback, "10.00", same)

	tests := []struct {
		name string
		op   *WagerTransaction
		ref  *WagerTransaction
		want FailureCode
	}{
		{"refund of bet", build(KindRefund, "10.00", same), bet, ""},
		{"rollback of bet", build(KindRollback, "10.00", same), bet, ""},
		{"rollback of win", build(KindRollback, "10.00", same), win, ""},
		{"rollback of refund", build(KindRollback, "10.00", same), refund, ""},
		{"win of bet", build(KindWin, "99.00", same), bet, ""},
		{"refund of win", build(KindRefund, "10.00", same), win, FailureReferenceKindInvalid},
		{"refund of refund", build(KindRefund, "10.00", same), refund, FailureReferenceKindInvalid},
		{"rollback of rollback", build(KindRollback, "10.00", same), rollback, FailureReferenceKindInvalid},
		{"win of win", build(KindWin, "10.00", same), win, FailureReferenceKindInvalid},
		{"partial refund", build(KindRefund, "5.00", same), bet, FailureReferenceAmountMismatch},
		{"partial rollback", build(KindRollback, "10.01", same), win, FailureReferenceAmountMismatch},
		{"other round", build(KindRefund, "10.00", func(p *ExternalParams) { same(p); p.RoundID = "round-2" }), bet, FailureReferenceMismatch},
		{"other wallet", build(KindRefund, "10.00", func(p *ExternalParams) { p.PlayerID = player; p.WalletID = NewID().String() }), bet, FailureReferenceMismatch},
		{"other player", build(KindRefund, "10.00", func(p *ExternalParams) { p.WalletID = wallet; p.PlayerID = NewID().String() }), bet, FailureReferenceMismatch},
		{"other provider", build(KindRefund, "10.00", func(p *ExternalParams) { same(p); p.ProviderID = "provider-b" }), bet, FailureReferenceMismatch},
		{"other currency", build(KindRefund, "10.00", func(p *ExternalParams) { same(p); p.Money = mustMoney(t, "10.00", "USD") }), bet, FailureReferenceMismatch},
	}
	for _, tc := range tests {
		got := tc.op.CheckReference(tc.ref)
		switch {
		case tc.want == "" && got != nil:
			t.Errorf("%s: unexpected rejection %+v", tc.name, got)
		case tc.want != "" && (got == nil || got.Code != tc.want):
			t.Errorf("%s: got %+v, want %s", tc.name, got, tc.want)
		}
	}
}

func TestMovementDirection(t *testing.T) {
	mk := func(k TransactionKind, amt string) *WagerTransaction {
		tx, err := NewExternalTransaction(baseParams(t, k, amt), testNow)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	bet, win, refund, loss := mk(KindBet, "1.00"), mk(KindWin, "1.00"), mk(KindRefund, "1.00"), mk(KindLoss, "0.00")
	rollback := mk(KindRollback, "1.00")

	check := func(name string, op, ref *WagerTransaction, want Direction, ok bool) {
		got, gotOK := op.MovementDirection(ref)
		if got != want || gotOK != ok {
			t.Errorf("%s: got (%s,%v), want (%s,%v)", name, got, gotOK, want, ok)
		}
	}
	check("bet", bet, nil, DirectionDebit, true)
	check("win", win, nil, DirectionCredit, true)
	check("refund", refund, bet, DirectionCredit, true)
	check("loss", loss, nil, "", false)
	check("rollback of bet", rollback, bet, DirectionCredit, true)
	check("rollback of win", rollback, win, DirectionDebit, true)
	check("rollback of refund", rollback, refund, DirectionDebit, true)
	check("rollback without ref", rollback, nil, "", false)

	if bet.InsufficientFundsCode() != FailureInsufficientFunds || rollback.InsufficientFundsCode() != FailureReversalInsufficientFunds {
		t.Error("insufficient-funds codes must differ between BET and reversals")
	}
	if loss.MovesMoney() || !bet.MovesMoney() {
		t.Error("MovesMoney")
	}
}

func TestCheckWallet(t *testing.T) {
	w := newTestWallet(t, "10.00")
	p := baseParams(t, KindBet, "1.00")
	p.WalletID, p.PlayerID = w.ID().String(), w.PlayerID().String()
	tx, _ := NewExternalTransaction(p, testNow)
	if r := tx.CheckWallet(w); r != nil {
		t.Fatalf("unexpected rejection %+v", r)
	}
	p.PlayerID = NewID().String()
	other, _ := NewExternalTransaction(p, testNow)
	if r := other.CheckWallet(w); r == nil || r.Code != FailureWalletPlayerMismatch {
		t.Errorf("player mismatch: %+v", r)
	}
	p.PlayerID = w.PlayerID().String()
	p.Money = mustMoney(t, "1.00", "USD")
	usd, _ := NewExternalTransaction(p, testNow)
	if r := usd.CheckWallet(w); r == nil || r.Code != FailureCurrencyMismatch {
		t.Errorf("currency mismatch: %+v", r)
	}
}

func TestEventEnvelopeJSON(t *testing.T) {
	op, err := OpenWallet(NewID(), mustMoney(t, "10.00", "BRL"), "corr-9", testNow)
	if err != nil {
		t.Fatal(err)
	}
	ev := op.Events[1]
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := env[key]; !ok {
			t.Errorf("envelope missing %q: %s", key, raw)
		}
	}
	var occurred string
	_ = json.Unmarshal(env["occurredAt"], &occurred)
	if _, err := time.Parse(time.RFC3339, occurred); err != nil || !strings.HasSuffix(occurred, "Z") {
		t.Errorf("occurredAt %q is not UTC RFC 3339", occurred)
	}
	var data map[string]any
	_ = json.Unmarshal(env["data"], &data)
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[key]; !ok {
			t.Errorf("WalletBalanceChanged data missing %q: %s", key, raw)
		}
	}
	rawProcessed, _ := json.Marshal(op.Events[0])
	for _, forbidden := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if strings.Contains(string(rawProcessed), forbidden) {
			t.Errorf("OPENING event leaks %q: %s", forbidden, rawProcessed)
		}
	}
}
