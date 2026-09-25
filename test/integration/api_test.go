//go:build integration

package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func single(t *testing.T, env map[string]string) (*stack, *instance) {
	t.Helper()
	s := newStack(t)
	return s, s.startProcess(t, "api", env)
}

func TestHealthEndpoints(t *testing.T) {
	_, inst := single(t, nil)
	live := call(t, http.MethodGet, inst.base+"/health/live", "", nil, nil)
	wantStatus(t, live, http.StatusOK)
	ready := call(t, http.MethodGet, inst.base+"/health/ready", "", nil, nil)
	wantStatus(t, ready, http.StatusOK)
	checks := ready.json(t)["checks"].(map[string]any)
	if checks["postgres"] != "UP" || checks["sqs"] != "UP" {
		t.Fatalf("checks = %v", checks)
	}
}

func TestAuthentication(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	o := newOp(w, "BET", "10.00")

	tests := []struct {
		name   string
		bearer string
		header map[string]string
		want   int
	}{
		{"missing credentials", "", nil, http.StatusUnauthorized},
		{"garbage token", "not-a-jwt", nil, http.StatusUnauthorized},
		{"tampered token", token(t, "provider-a") + "x", nil, http.StatusUnauthorized},
		{"wrong audience", token(t, "wrong-audience-client"), nil, http.StatusUnauthorized},
		{"no wagering role", token(t, "no-role-client"), nil, http.StatusForbidden},
		{"internal service cannot act as a provider", token(t, "wallet-service"), nil, http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := submitAs(t, inst.base, tc.bearer, o)
			wantStatus(t, r, tc.want)
			if tc.want == http.StatusUnauthorized && r.header.Get("WWW-Authenticate") == "" {
				t.Error("401 must carry WWW-Authenticate")
			}
		})
	}

	t.Run("basic scheme is not accepted", func(t *testing.T) {
		r := call(t, http.MethodPost, inst.base+"/wagering/transactions", "", map[string]string{
			"Authorization": "Basic " + token(t, "provider-a"), "Idempotency-Key": o.key}, o.payload())
		wantStatus(t, r, http.StatusUnauthorized)
	})

	t.Run("expired token", func(t *testing.T) {
		expiring := token(t, "expiring-provider")
		time.Sleep(3 * time.Second)
		wantStatus(t, submitAs(t, inst.base, expiring, o), http.StatusUnauthorized)
	})

	if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind <> 'OPENING'`, w.id); n != 0 {
		t.Fatalf("unauthorized calls created %d transactions", n)
	}
	if b := s.balanceMinor(w.id); b != 10000 {
		t.Fatalf("balance changed to %d", b)
	}
}

func TestInternalEndpointsAreRestrictedToTheInternalService(t *testing.T) {
	_, inst := single(t, nil)
	w := openWallet(t, inst.base, "50.00")
	provider := token(t, "provider-a")
	noRole := token(t, "no-role-client")

	endpoints := []struct{ method, path string }{
		{http.MethodGet, "/wallets/" + w.id},
		{http.MethodGet, "/wallets/" + w.id + "/ledger"},
		{http.MethodPost, "/wallets/" + w.id + "/reconciliation"},
	}
	for _, e := range endpoints {
		wantStatus(t, call(t, e.method, inst.base+e.path, "", nil, nil), http.StatusUnauthorized)
		wantStatus(t, call(t, e.method, inst.base+e.path, provider, nil, nil), http.StatusForbidden)
		wantStatus(t, call(t, e.method, inst.base+e.path, noRole, nil, nil), http.StatusForbidden)
	}
	r := call(t, http.MethodPost, inst.base+"/wallets", provider, nil, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "999.00", "currency": "BRL"}})
	wantStatus(t, r, http.StatusForbidden)
	if strings.Contains(string(r.body), "999.00") {
		t.Error("a forbidden response must not echo the request")
	}
}

func TestProviderIsolation(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")

	a := newOp(w, "BET", "10.00")
	first := submit(t, inst.base, a)
	wantStatus(t, first, http.StatusOK)
	txID := first.str(t, "transactionId")

	providerA, providerB := token(t, "provider-a"), token(t, "provider-b")

	wantStatus(t, call(t, http.MethodGet, inst.base+"/wagering/transactions/"+txID, providerA, nil, nil), http.StatusOK)
	wantStatus(t, call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+a.ext, providerA, nil, nil), http.StatusOK)

	r := call(t, http.MethodGet, inst.base+"/wagering/transactions/"+txID, providerB, nil, nil)
	wantStatus(t, r, http.StatusNotFound)
	if strings.Contains(string(r.body), "provider-a") || strings.Contains(string(r.body), a.ext) {
		t.Error("404 body leaks data of another provider")
	}
	wantStatus(t, call(t, http.MethodGet, inst.base+"/providers/provider-a/wagering/transactions/"+a.ext, providerB, nil, nil), http.StatusForbidden)
	wantStatus(t, call(t, http.MethodGet, inst.base+"/providers/provider-b/wagering/transactions/"+a.ext, providerB, nil, nil), http.StatusNotFound)

	wantStatus(t, call(t, http.MethodGet, inst.base+"/wagering/transactions/"+txID, token(t, "wallet-service"), nil, nil), http.StatusOK)

	forged := a
	forged.provider = "provider-a"
	wantStatus(t, submitAs(t, inst.base, providerB, forged), http.StatusForbidden)
	if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.id); n != 1 {
		t.Fatalf("forged request changed the ledger: %d bets", n)
	}
	if b := s.balanceMinor(w.id); b != 9000 {
		t.Fatalf("balance = %d", b)
	}

	replay := submit(t, inst.base, a)
	wantStatus(t, replay, http.StatusOK)
	if replay.json(t)["idempotentReplay"] != true {
		t.Error("expected replay")
	}
}

func TestWalletOpening(t *testing.T) {
	s, inst := single(t, nil)

	t.Run("positive balance creates OPENING, ledger credit and events", func(t *testing.T) {
		w := openWallet(t, inst.base, "1000.00")
		if s.queryInt(`SELECT version FROM wallets WHERE id=$1`, w.id) != 1 {
			t.Error("wallet version must be 1")
		}
		if got := s.queryString(`SELECT status FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING'`, w.id); got != "PROCESSED" {
			t.Errorf("OPENING status = %s", got)
		}
		if got := s.queryString(`SELECT origin FROM wager_transactions WHERE wallet_id=$1 AND kind='OPENING'`, w.id); got != "INTERNAL" {
			t.Errorf("origin = %s", got)
		}
		if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='CREDIT' AND balance_before_minor=0 AND balance_after_minor=100000 AND wallet_version=1`, w.id); n != 1 {
			t.Errorf("opening credit entries = %d", n)
		}
		if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id=$1) AND event_type='WagerTransactionProcessed'`, w.id); n != 1 {
			t.Errorf("WagerTransactionProcessed events = %d", n)
		}
		if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='WalletBalanceChanged'`, w.id); n != 1 {
			t.Errorf("WalletBalanceChanged events = %d", n)
		}
	})

	t.Run("zero balance creates no financial records", func(t *testing.T) {
		w := openWallet(t, inst.base, "0.00")
		if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE wallet_id=$1`, w.id); n != 0 {
			t.Errorf("transactions = %d", n)
		}
		if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, w.id); n != 0 {
			t.Errorf("ledger entries = %d", n)
		}
		if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE partition_key=$1`, w.id); n != 0 {
			t.Errorf("events = %d", n)
		}
	})

	t.Run("second wallet for the same player and currency conflicts", func(t *testing.T) {
		w := openWallet(t, inst.base, "10.00")
		r := call(t, http.MethodPost, inst.base+"/wallets", token(t, "wallet-service"), nil, map[string]any{
			"playerId": w.player, "initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"}})
		wantStatus(t, r, http.StatusConflict)
		if r.str(t, "error", "code") != "WALLET_ALREADY_EXISTS" {
			t.Errorf("body = %s", r.body)
		}
		r = call(t, http.MethodPost, inst.base+"/wallets", token(t, "wallet-service"), nil, map[string]any{
			"playerId": w.player, "initialBalance": map[string]string{"amount": "5.00", "currency": "USD"}})
		wantStatus(t, r, http.StatusCreated)
	})

	t.Run("invalid input", func(t *testing.T) {
		bad := map[string]any{
			"negative":      map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]any{"amount": "-1.00", "currency": "BRL"}},
			"float amount":  `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":10.5,"currency":"BRL"}}`,
			"scale":         map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]any{"amount": "1.001", "currency": "BRL"}},
			"bad currency":  map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]any{"amount": "1.00", "currency": "XXX"}},
			"bad player":    map[string]any{"playerId": "nope", "initialBalance": map[string]any{"amount": "1.00", "currency": "BRL"}},
			"missing money": map[string]any{"playerId": uuid.NewString()},
			"unknown field": map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]any{"amount": "1.00", "currency": "BRL"}, "x": 1},
		}
		for name, body := range bad {
			r := call(t, http.MethodPost, inst.base+"/wallets", token(t, "wallet-service"), nil, body)
			wantStatus(t, r, http.StatusBadRequest)
			if code := r.str(t, "error", "code"); code != "INVALID_REQUEST" && code != "INVALID_MONEY" {
				t.Errorf("%s: code = %s", name, code)
			}
		}
	})

	t.Run("reading", func(t *testing.T) {
		w := openWallet(t, inst.base, "12.34")
		r := call(t, http.MethodGet, inst.base+"/wallets/"+w.id, token(t, "wallet-service"), nil, nil)
		wantStatus(t, r, http.StatusOK)
		if r.str(t, "balance", "amount") != "12.34" || r.json(t)["version"].(float64) != 1 {
			t.Errorf("wallet = %s", r.body)
		}
		wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/"+uuid.NewString(), token(t, "wallet-service"), nil, nil), http.StatusNotFound)
		wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/not-a-uuid", token(t, "wallet-service"), nil, nil), http.StatusNotFound)
	})
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	for i := 0; i < 6; i++ {
		wantStatus(t, submit(t, inst.base, newOp(w, "BET", "1.00")), http.StatusOK)
	}
	wantStatus(t, submit(t, inst.base, newOp(w, "LOSS", "0.00")), http.StatusOK)
	internal := token(t, "wallet-service")

	var seen []float64
	cursor := ""
	for page := 0; page < 10; page++ {
		url := inst.base + "/wallets/" + w.id + "/ledger?limit=3"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		r := call(t, http.MethodGet, url, internal, nil, nil)
		wantStatus(t, r, http.StatusOK)
		body := r.json(t)
		for _, e := range body["entries"].([]any) {
			seen = append(seen, e.(map[string]any)["walletVersion"].(float64))
		}
		next, _ := body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	want := []float64{7, 6, 5, 4, 3, 2, 1}
	if len(seen) != len(want) {
		t.Fatalf("versions = %v", seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("versions = %v, want %v", seen, want)
		}
	}
	wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/"+w.id+"/ledger?cursor=garbage", internal, nil, nil), http.StatusBadRequest)
	wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/"+w.id+"/ledger?limit=0", internal, nil, nil), http.StatusBadRequest)
	wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/"+w.id+"/ledger?limit=500", internal, nil, nil), http.StatusBadRequest)
	wantStatus(t, call(t, http.MethodGet, inst.base+"/wallets/"+uuid.NewString()+"/ledger", internal, nil, nil), http.StatusNotFound)

	rec := call(t, http.MethodPost, inst.base+"/wallets/"+w.id+"/reconciliation", internal, nil, nil)
	wantStatus(t, rec, http.StatusOK)
	j := rec.json(t)
	if j["consistent"] != true || j["checkedEntries"].(float64) != 7 ||
		rec.str(t, "storedBalance", "amount") != "94.00" || rec.str(t, "calculatedBalance", "amount") != "94.00" ||
		rec.str(t, "difference", "amount") != "0.00" {
		t.Fatalf("reconciliation = %s", rec.body)
	}

	if err := s.exec(`ALTER TABLE wallets DISABLE TRIGGER USER`); err != nil {
		t.Fatalf("disable triggers: %v", err)
	}
	if err := s.exec(`UPDATE wallets SET balance_minor = balance_minor + 5 WHERE id = $1`, w.id); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	rec = call(t, http.MethodPost, inst.base+"/wallets/"+w.id+"/reconciliation", internal, nil, nil)
	wantStatus(t, rec, http.StatusOK)
	if rec.json(t)["consistent"] != false || rec.str(t, "difference", "amount") != "0.05" {
		t.Fatalf("divergence not reported: %s", rec.body)
	}
	if b := s.balanceMinor(w.id); b != 9405 {
		t.Fatalf("reconciliation changed the balance: %d", b)
	}
	if !strings.Contains(inst.out.String(), "wallet reconciliation divergence") {
		t.Error("divergence must be logged")
	}
	m := call(t, http.MethodGet, inst.metrics+"/metrics", "", nil, nil)
	if !strings.Contains(string(m.body), "wagering_reconciliation_divergences_total 1") {
		t.Error("divergence metric missing")
	}
}
