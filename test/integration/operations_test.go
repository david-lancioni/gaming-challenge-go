//go:build integration

package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func amountOf(t testing.TB, r response, field string) string {
	t.Helper()
	return r.str(t, field, "amount")
}

func TestFiveOperationKinds(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")

	bet := newOp(w, "BET", "30.00")
	r := submit(t, inst.base, bet)
	wantStatus(t, r, http.StatusOK)
	if r.str(t, "status") != "PROCESSED" || amountOf(t, r, "balance") != "70.00" || r.json(t)["idempotentReplay"] != false {
		t.Fatalf("BET = %s", r.body)
	}

	win := newOp(w, "WIN", "50.00")
	win.reference = bet.ext
	r = submit(t, inst.base, win)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "120.00" {
		t.Fatalf("WIN = %s", r.body)
	}

	versionBefore := s.queryInt(`SELECT version FROM wallets WHERE id=$1`, w.id)
	entriesBefore := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, w.id)
	loss := newOp(w, "LOSS", "0.00")
	r = submit(t, inst.base, loss)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "120.00" {
		t.Fatalf("LOSS = %s", r.body)
	}
	if s.queryInt(`SELECT version FROM wallets WHERE id=$1`, w.id) != versionBefore ||
		s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, w.id) != entriesBefore {
		t.Fatal("LOSS must neither create ledger entries nor change the wallet version")
	}

	refundedBet := newOp(w, "BET", "20.00")
	wantStatus(t, submit(t, inst.base, refundedBet), http.StatusOK)
	refund := newOp(w, "REFUND", "20.00")
	refund.reference = refundedBet.ext
	r = submit(t, inst.base, refund)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "120.00" {
		t.Fatalf("REFUND = %s", r.body)
	}

	rollbackWin := newOp(w, "ROLLBACK", "50.00")
	rollbackWin.reference = win.ext
	r = submit(t, inst.base, rollbackWin)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "70.00" {
		t.Fatalf("ROLLBACK of WIN = %s", r.body)
	}

	rollbackBet := newOp(w, "ROLLBACK", "30.00")
	rollbackBet.reference = bet.ext
	r = submit(t, inst.base, rollbackBet)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "100.00" {
		t.Fatalf("ROLLBACK of BET = %s", r.body)
	}

	tx := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+rollbackBet.ext, token(t, "provider-a"), nil, nil)
	wantStatus(t, tx, http.StatusOK)
	betTx := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+bet.ext, token(t, "provider-a"), nil, nil)
	if tx.str(t, "referenceTransactionId") != betTx.str(t, "transactionId") {
		t.Errorf("resolved reference = %s, want %s", tx.str(t, "referenceTransactionId"), betTx.str(t, "transactionId"))
	}
	s.assertConsistent(w.id)
}

func TestInsufficientFundsAndReversalCodesDiffer(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "0.00")
	credit := newOp(w, "WIN", "50.00")
	wantStatus(t, submit(t, inst.base, credit), http.StatusOK)

	poor := newOp(w, "BET", "80.00")
	r := submit(t, inst.base, poor)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if r.str(t, "status") != "REJECTED" || r.str(t, "failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("BET = %s", r.body)
	}

	wantStatus(t, submit(t, inst.base, newOp(w, "BET", "40.00")), http.StatusOK)
	rb := newOp(w, "ROLLBACK", "50.00")
	rb.reference = credit.ext
	r = submit(t, inst.base, rb)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if r.str(t, "failureCode") != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("ROLLBACK = %s", r.body)
	}

	if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND status='REJECTED'`, w.id); n != 2 {
		t.Errorf("rejected transactions = %d", n)
	}
	if s.balanceMinor(w.id) != 1000 {
		t.Errorf("balance = %d", s.balanceMinor(w.id))
	}
	s.assertConsistent(w.id)

	replay := submit(t, inst.base, poor)
	wantStatus(t, replay, http.StatusUnprocessableEntity)
	if replay.json(t)["idempotentReplay"] != true || replay.str(t, "failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("replay = %s", replay.body)
	}
	wantStatus(t, submit(t, inst.base, newOp(w, "WIN", "500.00")), http.StatusOK)
	again := submit(t, inst.base, poor)
	wantStatus(t, again, http.StatusUnprocessableEntity)
	if again.json(t)["idempotentReplay"] != true {
		t.Fatal("expected a replay of the rejection")
	}
}

func TestReversalRules(t *testing.T) {
	_, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	code := func(r response) string { return r.str(t, "failureCode") }

	bet := newOp(w, "BET", "25.00")
	wantStatus(t, submit(t, inst.base, bet), http.StatusOK)
	refund := newOp(w, "REFUND", "25.00")
	refund.reference = bet.ext
	r := submit(t, inst.base, refund)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "100.00" {
		t.Fatalf("refund = %s", r.body)
	}
	dup := newOp(w, "REFUND", "25.00")
	dup.reference = bet.ext
	r = submit(t, inst.base, dup)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if code(r) != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("second refund = %s", r.body)
	}
	rb := newOp(w, "ROLLBACK", "25.00")
	rb.reference = bet.ext
	r = submit(t, inst.base, rb)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if code(r) != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("rollback after refund of the same debit = %s", r.body)
	}

	bet2 := newOp(w, "BET", "10.00")
	wantStatus(t, submit(t, inst.base, bet2), http.StatusOK)
	rb2 := newOp(w, "ROLLBACK", "10.00")
	rb2.reference = bet2.ext
	wantStatus(t, submit(t, inst.base, rb2), http.StatusOK)
	rf2 := newOp(w, "REFUND", "10.00")
	rf2.reference = bet2.ext
	r = submit(t, inst.base, rf2)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if code(r) != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("refund after rollback = %s", r.body)
	}

	rbRefund := newOp(w, "ROLLBACK", "25.00")
	rbRefund.reference = refund.ext
	r = submit(t, inst.base, rbRefund)
	wantStatus(t, r, http.StatusOK)
	if amountOf(t, r, "balance") != "75.00" {
		t.Fatalf("rollback of refund = %s", r.body)
	}
	rbrb := newOp(w, "ROLLBACK", "25.00")
	rbrb.reference = rbRefund.ext
	r = submit(t, inst.base, rbrb)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if code(r) != "REFERENCE_KIND_INVALID" {
		t.Fatalf("rollback of rollback = %s", r.body)
	}

	win := newOp(w, "WIN", "5.00")
	wantStatus(t, submit(t, inst.base, win), http.StatusOK)
	rfWin := newOp(w, "REFUND", "5.00")
	rfWin.reference = win.ext
	if code(submit(t, inst.base, rfWin)) != "REFERENCE_KIND_INVALID" {
		t.Error("refund of a WIN must be rejected")
	}
	bet3 := newOp(w, "BET", "8.00")
	wantStatus(t, submit(t, inst.base, bet3), http.StatusOK)
	partial := newOp(w, "REFUND", "4.00")
	partial.reference = bet3.ext
	if code(submit(t, inst.base, partial)) != "REFERENCE_AMOUNT_MISMATCH" {
		t.Error("partial refund must be rejected")
	}

	otherRound := newOp(w, "REFUND", "8.00")
	otherRound.reference, otherRound.round = bet3.ext, "round-other"
	if code(submit(t, inst.base, otherRound)) != "REFERENCE_MISMATCH" {
		t.Error("different round must be rejected")
	}
	w2 := openWallet(t, inst.base, "50.00")
	otherWallet := newOp(w2, "REFUND", "8.00")
	otherWallet.reference = bet3.ext
	if code(submit(t, inst.base, otherWallet)) != "REFERENCE_MISMATCH" {
		t.Error("different wallet must be rejected")
	}

	foreign := newOp(w, "REFUND", "8.00")
	foreign.provider, foreign.key = "provider-b", "provider-b:"+foreign.ext
	foreign.reference = bet3.ext
	r = submit(t, inst.base, foreign)
	wantStatus(t, r, http.StatusAccepted)
	if r.str(t, "status") != "PENDING_REFERENCE" {
		t.Fatalf("cross-provider reference = %s", r.body)
	}

	poor := newOp(w2, "BET", "999.00")
	wantStatus(t, submit(t, inst.base, poor), http.StatusUnprocessableEntity)
	rfPoor := newOp(w2, "REFUND", "999.00")
	rfPoor.reference = poor.ext
	r = submit(t, inst.base, rfPoor)
	wantStatus(t, r, http.StatusUnprocessableEntity)
	if code(r) != "REFERENCE_NOT_PROCESSED" {
		t.Fatalf("refund of a rejected bet = %s", r.body)
	}
}

func TestIdempotencyContract(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	bet := newOp(w, "BET", "10.00")

	first := submit(t, inst.base, bet)
	wantStatus(t, first, http.StatusOK)
	txID := first.str(t, "transactionId")

	wantStatus(t, submit(t, inst.base, newOp(w, "WIN", "60.00")), http.StatusOK)
	replay := submit(t, inst.base, bet)
	wantStatus(t, replay, http.StatusOK)
	if replay.str(t, "transactionId") != txID || amountOf(t, replay, "balance") != "90.00" || replay.json(t)["idempotentReplay"] != true {
		t.Fatalf("replay = %s", replay.body)
	}

	changed := bet
	changed.amount = "11.00"
	r := submit(t, inst.base, changed)
	wantStatus(t, r, http.StatusConflict)
	if r.str(t, "error", "code") != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("conflict = %s", r.body)
	}
	other := bet
	other.key = "another-key-" + uuid.NewString()
	r = submit(t, inst.base, other)
	wantStatus(t, r, http.StatusConflict)
	if r.str(t, "error", "code") != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("conflict = %s", r.body)
	}
	custom := newOp(w, "BET", "1.00")
	custom.key = "custom-key-not-derived-from-ids"
	wantStatus(t, submit(t, inst.base, custom), http.StatusOK)
	if s.queryString(`SELECT idempotency_key FROM wager_transactions WHERE external_transaction_id=$1`, custom.ext) != custom.key {
		t.Error("the idempotency key was not stored as received")
	}

	if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id=$1`, txID); n != 1 {
		t.Fatalf("ledger entries for the bet = %d", n)
	}
	r = call(t, http.MethodPost, inst.base+"/wagering/transactions", token(t, "provider-a"), nil, bet.payload())
	wantStatus(t, r, http.StatusBadRequest)
	s.assertConsistent(w.id)
}

func TestInputValidationHTTP(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	base := newOp(w, "BET", "10.00")
	before := s.queryInt(`SELECT count(*) FROM wager_transactions`)

	mutate := func(f func(p map[string]any)) map[string]any {
		p := base.payload()
		f(p)
		return p
	}
	cases := map[string]any{
		"OPENING kind":  mutate(func(p map[string]any) { p["kind"] = "OPENING" }),
		"unknown kind":  mutate(func(p map[string]any) { p["kind"] = "TRANSFER" }),
		"BET zero":      mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "0.00", "currency": "BRL"} }),
		"LOSS positive": mutate(func(p map[string]any) { p["kind"] = "LOSS" }),
		"negative":      mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "-1.00", "currency": "BRL"} }),
		"scale":         mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "1.001", "currency": "BRL"} }),
		"scientific":    mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "1e1", "currency": "BRL"} }),
		"NaN":           mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "NaN", "currency": "BRL"} }),
		"Infinity":      mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "Infinity", "currency": "BRL"} }),
		"empty amount":  mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "", "currency": "BRL"} }),
		"bad currency":  mutate(func(p map[string]any) { p["money"] = map[string]string{"amount": "1.00", "currency": "brl"} }),
		"overflow": mutate(func(p map[string]any) {
			p["money"] = map[string]string{"amount": "99999999999999999999.00", "currency": "BRL"}
		}),
		"missing money":        mutate(func(p map[string]any) { delete(p, "money") }),
		"missing round":        mutate(func(p map[string]any) { delete(p, "roundId") }),
		"bad wallet id":        mutate(func(p map[string]any) { p["walletId"] = "x" }),
		"refund w/o reference": mutate(func(p map[string]any) { p["kind"] = "REFUND" }),
		"BET with reference":   mutate(func(p map[string]any) { p["referenceExternalTransactionId"] = "other" }),
		"unknown field":        mutate(func(p map[string]any) { p["extra"] = true }),
		"amount as number":     `{"providerId":"provider-a","externalTransactionId":"n1","playerId":"` + w.player + `","walletId":"` + w.id + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":10.5,"currency":"BRL"}}`,
		"not json":             `{{{`,
		"trailing data":        `{"providerId":"provider-a"} {"x":1}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			r := call(t, http.MethodPost, inst.base+"/wagering/transactions", token(t, "provider-a"),
				map[string]string{"Idempotency-Key": "k-" + uuid.NewString()}, body)
			wantStatus(t, r, http.StatusBadRequest)
			if r.str(t, "error", "code") == "" || strings.Contains(string(r.body), "goroutine") {
				t.Errorf("unexpected body: %s", r.body)
			}
		})
	}
	t.Run("wrong content type", func(t *testing.T) {
		r := call(t, http.MethodPost, inst.base+"/wagering/transactions", token(t, "provider-a"),
			map[string]string{"Idempotency-Key": "k", "Content-Type": "text/plain"}, base.payload())
		wantStatus(t, r, http.StatusUnsupportedMediaType)
	})
	t.Run("oversized body", func(t *testing.T) {
		r := call(t, http.MethodPost, inst.base+"/wagering/transactions", token(t, "provider-a"),
			map[string]string{"Idempotency-Key": "k"}, `{"providerId":"`+strings.Repeat("a", 100_000)+`"}`)
		wantStatus(t, r, http.StatusRequestEntityTooLarge)
	})
	t.Run("unknown wallet", func(t *testing.T) {
		ghost := walletRef{id: uuid.NewString(), player: uuid.NewString()}
		r := submit(t, inst.base, newOp(ghost, "BET", "1.00"))
		wantStatus(t, r, http.StatusNotFound)
		if r.str(t, "error", "code") != "WALLET_NOT_FOUND" {
			t.Errorf("body = %s", r.body)
		}
	})
	t.Run("wallet of another player", func(t *testing.T) {
		wrong := w
		wrong.player = uuid.NewString()
		r := submit(t, inst.base, newOp(wrong, "BET", "1.00"))
		wantStatus(t, r, http.StatusUnprocessableEntity)
		if r.str(t, "failureCode") != "WALLET_PLAYER_MISMATCH" {
			t.Errorf("body = %s", r.body)
		}
	})
	t.Run("wallet currency", func(t *testing.T) {
		o := newOp(w, "BET", "1.00")
		p := o.payload()
		p["money"] = map[string]string{"amount": "1.00", "currency": "USD"}
		r := call(t, http.MethodPost, inst.base+"/wagering/transactions", token(t, "provider-a"),
			map[string]string{"Idempotency-Key": o.key}, p)
		wantStatus(t, r, http.StatusUnprocessableEntity)
		if r.str(t, "failureCode") != "CURRENCY_MISMATCH" {
			t.Errorf("body = %s", r.body)
		}
	})

	if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE status='REJECTED'`); n != 2 {
		t.Errorf("rejected = %d", n)
	}
	if got := s.queryInt(`SELECT count(*) FROM wager_transactions`); got != before+2 {
		t.Errorf("transactions grew by %d, want 2", got-before)
	}
	if s.balanceMinor(w.id) != 10000 {
		t.Errorf("balance = %d", s.balanceMinor(w.id))
	}
}

func TestPendingReferenceResolvesWhenTheReferenceArrives(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	bet := newOp(w, "BET", "40.00")
	refund := newOp(w, "REFUND", "40.00")
	refund.reference = bet.ext

	r := submit(t, inst.base, refund)
	wantStatus(t, r, http.StatusAccepted)
	if r.str(t, "status") != "PENDING_REFERENCE" || r.header.Get("Location") == "" {
		t.Fatalf("pending = %s", r.body)
	}
	txID := r.str(t, "transactionId")
	rp := submit(t, inst.base, refund)
	wantStatus(t, rp, http.StatusAccepted)
	if rp.json(t)["idempotentReplay"] != true || rp.str(t, "transactionId") != txID {
		t.Fatalf("replay = %s", rp.body)
	}
	get := call(t, http.MethodGet, inst.base+"/wagering/transactions/"+txID, token(t, "provider-a"), nil, nil)
	wantStatus(t, get, http.StatusOK)
	if get.str(t, "status") != "PENDING_REFERENCE" || get.str(t, "expiresAt") == "" {
		t.Fatalf("get = %s", get.body)
	}
	if s.balanceMinor(w.id) != 10000 {
		t.Fatal("a pending operation must not move money")
	}

	wantStatus(t, submit(t, inst.base, bet), http.StatusOK)
	eventually(t, 20*time.Second, "refund to be processed", func() bool {
		g := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+refund.ext, token(t, "provider-a"), nil, nil)
		return g.str(t, "status") == "PROCESSED"
	})
	if s.balanceMinor(w.id) != 10000 {
		t.Fatalf("balance after bet+refund = %d", s.balanceMinor(w.id))
	}
	final := call(t, http.MethodGet, inst.base+"/wagering/transactions/"+txID, token(t, "provider-a"), nil, nil)
	if final.str(t, "balance", "amount") != "100.00" || final.str(t, "referenceTransactionId") == "" {
		t.Fatalf("final = %s", final.body)
	}
	rp = submit(t, inst.base, refund)
	wantStatus(t, rp, http.StatusOK)

	evs := parseEvents(t, s.drain(t, s.urls.Events, 15*time.Second, func(m []messageT) bool {
		return countType(parseEvents(t, m), "WagerTransactionProcessed") >= 3
	}))
	mine := eventsOf(evs, w.id)
	if countType(mine, "WagerTransactionPendingReference") != 1 {
		t.Errorf("pending-reference events = %d", countType(mine, "WagerTransactionPendingReference"))
	}
	s.assertConsistent(w.id)
}

func TestPendingReferenceExpiresAsRejected(t *testing.T) {
	s, inst := single(t, map[string]string{"PENDING_TTL": "3s", "PENDING_MAX_ATTEMPTS": "50"})
	w := openWallet(t, inst.base, "100.00")

	missing := newOp(w, "ROLLBACK", "10.00")
	missing.reference = "never-arrives"
	wantStatus(t, submit(t, inst.base, missing), http.StatusAccepted)

	eventually(t, 25*time.Second, "expiration", func() bool {
		g := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+missing.ext, token(t, "provider-a"), nil, nil)
		return g.str(t, "status") == "REJECTED"
	})
	g := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+missing.ext, token(t, "provider-a"), nil, nil)
	if g.str(t, "failureCode") != "REFERENCE_NOT_FOUND" {
		t.Fatalf("expired = %s", g.body)
	}
	rp := submit(t, inst.base, missing)
	wantStatus(t, rp, http.StatusUnprocessableEntity)
	if s.balanceMinor(w.id) != 10000 {
		t.Fatal("expiration must not move money")
	}
	evs := parseEvents(t, s.drain(t, s.urls.Events, 15*time.Second, func(m []messageT) bool {
		return countType(eventsOf(parseEvents(t, m), w.id), "WagerTransactionRejected") >= 1
	}))
	rej := eventsOf(evs, w.id)
	if countType(rej, "WagerTransactionRejected") != 1 {
		t.Fatalf("rejection events = %d", countType(rej, "WagerTransactionRejected"))
	}
	for _, e := range rej {
		if e.EventType == "WagerTransactionRejected" && e.Data["failureCode"] != "REFERENCE_NOT_FOUND" {
			t.Errorf("event = %+v", e)
		}
	}
}

func TestPendingReferenceBudgetByAttempts(t *testing.T) {
	_, inst := single(t, map[string]string{"PENDING_TTL": "10m", "PENDING_MAX_ATTEMPTS": "3"})
	w := openWallet(t, inst.base, "100.00")

	missing := newOp(w, "REFUND", "10.00")
	missing.reference = "never-arrives"
	wantStatus(t, submit(t, inst.base, missing), http.StatusAccepted)
	eventually(t, 25*time.Second, "attempt budget exhaustion", func() bool {
		g := call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+missing.ext, token(t, "provider-a"), nil, nil)
		return g.str(t, "status") == "REJECTED" && g.str(t, "failureCode") == "REFERENCE_NOT_FOUND"
	})
}

func TestReferenceThatEndsWithoutSuccess(t *testing.T) {
	_, inst := single(t, map[string]string{"PENDING_TTL": "3s"})
	w := openWallet(t, inst.base, "100.00")

	b := newOp(w, "ROLLBACK", "10.00")
	b.reference = "never-arrives"
	wantStatus(t, submit(t, inst.base, b), http.StatusAccepted)
	a := newOp(w, "REFUND", "10.00")
	a.reference = b.ext
	wantStatus(t, submit(t, inst.base, a), http.StatusAccepted)

	get := func(o op) response {
		return call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+o.ext, token(t, "provider-a"), nil, nil)
	}
	eventually(t, 25*time.Second, "both to be rejected", func() bool {
		return get(a).str(t, "status") == "REJECTED" && get(b).str(t, "status") == "REJECTED"
	})
	if c := get(b).str(t, "failureCode"); c != "REFERENCE_NOT_FOUND" {
		t.Errorf("B = %s", c)
	}
	if c := get(a).str(t, "failureCode"); c != "REFERENCE_NOT_PROCESSED" && c != "REFERENCE_UNAVAILABLE" {
		t.Errorf("A = %s", c)
	}
}
