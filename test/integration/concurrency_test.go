//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func cluster(t *testing.T, n int, env map[string]string) (*stack, []*instance) {
	t.Helper()
	s := newStack(t)
	insts := make([]*instance, n)
	for i := range insts {
		insts[i] = s.spawn(t, fmt.Sprintf("api-%d", i+1), env)
	}
	for _, in := range insts {
		in.waitReady(t, 60*time.Second)
	}
	return s, insts
}

func fanOut(n int, fn func(i int)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
}

func TestSameBetFiftyTimesInParallelAcrossThreeProcesses(t *testing.T) {
	s, insts := cluster(t, 3, nil)
	w := openWallet(t, insts[0].base, "1000.00")
	bet := newOp(w, "BET", "25.00")

	const n = 50
	results := make([]response, n)
	fanOut(n, func(i int) {
		results[i] = submit(t, insts[i%3].base, bet)
	})

	txIDs := map[string]int{}
	fresh := 0
	for i, r := range results {
		if r.status != http.StatusOK {
			t.Fatalf("request %d: %d %s", i, r.status, r.body)
		}
		txIDs[r.str(t, "transactionId")]++
		if amountOf(t, r, "balance") != "975.00" {
			t.Fatalf("request %d balance = %s", i, r.body)
		}
		if r.json(t)["idempotentReplay"] == false {
			fresh++
		}
	}
	if len(txIDs) != 1 {
		t.Fatalf("distinct transaction ids = %v", txIDs)
	}
	if fresh != 1 {
		t.Fatalf("%d responses claim to be the original, want exactly 1", fresh)
	}
	if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, w.id); n != 1 {
		t.Fatalf("debits = %d, want 1", n)
	}
	if b := s.balanceMinor(w.id); b != 97500 {
		t.Fatalf("balance = %d", b)
	}
	s.assertConsistent(w.id)
}

func TestTwoBetsOf80OnABalanceOf100(t *testing.T) {
	s, insts := cluster(t, 3, nil)

	for round := 0; round < 12; round++ {
		w := openWallet(t, insts[0].base, "100.00")
		a, b := newOp(w, "BET", "80.00"), newOp(w, "BET", "80.00")
		out := make([]response, 2)
		fanOut(2, func(i int) {
			o := []op{a, b}[i]
			out[i] = submit(t, insts[(round+i)%3].base, o)
		})

		processed, rejected := 0, 0
		for _, r := range out {
			switch r.status {
			case http.StatusOK:
				processed++
			case http.StatusUnprocessableEntity:
				rejected++
				if r.str(t, "failureCode") != "INSUFFICIENT_FUNDS" {
					t.Fatalf("round %d: %s", round, r.body)
				}
			default:
				t.Fatalf("round %d: unexpected %d %s", round, r.status, r.body)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("round %d: processed=%d rejected=%d", round, processed, rejected)
		}
		if bal := s.balanceMinor(w.id); bal != 2000 {
			t.Fatalf("round %d: balance = %d, want 2000", round, bal)
		}
		if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, w.id); n != 1 {
			t.Fatalf("round %d: debits = %d", round, n)
		}

		for i, o := range []op{a, b} {
			again := submit(t, insts[(round+i+1)%3].base, o)
			if again.status != out[i].status || again.json(t)["idempotentReplay"] != true {
				t.Fatalf("round %d: resubmission of %d: %d %s", round, i, again.status, again.body)
			}
		}
		if bal := s.balanceMinor(w.id); bal != 2000 {
			t.Fatalf("round %d: balance after resubmissions = %d", round, bal)
		}
		s.assertConsistent(w.id)
	}
}

func TestIndependentWalletsAreNotBlockedByAnotherWalletsLock(t *testing.T) {
	s, insts := cluster(t, 2, nil)
	locked := openWallet(t, insts[0].base, "100.00")
	free := openWallet(t, insts[0].base, "100.00")

	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE`, locked.id); err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			_ = tx.Rollback(context.Background())
		}
	}()

	blocked := make(chan response, 1)
	go func() { blocked <- submit(t, insts[0].base, newOp(locked, "BET", "10.00")) }()
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 10; i++ {
		start := time.Now()
		wantStatus(t, submit(t, insts[i%2].base, newOp(free, "BET", "1.00")), http.StatusOK)
		if d := time.Since(start); d > 3*time.Second {
			t.Fatalf("independent wallet took %s while another wallet was locked", d)
		}
	}
	select {
	case r := <-blocked:
		t.Fatalf("the locked wallet was processed while its row lock was held: %d %s", r.status, r.body)
	default:
	}

	released = true
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-blocked:
		wantStatus(t, r, http.StatusOK)
	case <-time.After(20 * time.Second):
		t.Fatal("the locked wallet did not resume after the lock was released")
	}
	s.assertConsistent(locked.id)
	s.assertConsistent(free.id)
}

func TestManyWalletsConcurrentlyAcrossThreeProcesses(t *testing.T) {
	s, insts := cluster(t, 3, nil)
	const wallets, betsPer = 30, 5
	ws := make([]walletRef, wallets)
	for i := range ws {
		ws[i] = openWallet(t, insts[i%3].base, "100.00")
	}

	var mu sync.Mutex
	var failures []string
	fanOut(wallets*betsPer, func(i int) {
		w := ws[i%wallets]
		r := submitRetry(t, insts[i%3].base, newOp(w, "BET", "10.00"))
		if r.status != http.StatusOK {
			mu.Lock()
			failures = append(failures, fmt.Sprintf("%d: %s", r.status, r.body))
			mu.Unlock()
		}
	})
	if len(failures) > 0 {
		t.Fatalf("%d failures, first: %s", len(failures), failures[0])
	}
	for _, w := range ws {
		if b := s.balanceMinor(w.id); b != 5000 {
			t.Fatalf("wallet %s balance = %d", w.id, b)
		}
		s.assertConsistent(w.id)
	}
}

func TestRandomWorkloadKeepsFinancialInvariants(t *testing.T) {
	s, insts := cluster(t, 3, nil)
	const wallets = 8
	ws := make([]walletRef, wallets)
	for i := range ws {
		ws[i] = openWallet(t, insts[0].base, "200.00")
	}
	rng := rand.New(rand.NewSource(42))
	type job struct{ o op }
	var jobs []job
	for _, w := range ws {
		for i := 0; i < 25; i++ {
			amount := fmt.Sprintf("%d.%02d", 1+rng.Intn(60), rng.Intn(100))
			bet := newOp(w, "BET", amount)
			jobs = append(jobs, job{bet})
			switch rng.Intn(5) {
			case 0:
				win := newOp(w, "WIN", fmt.Sprintf("%d.%02d", 1+rng.Intn(80), rng.Intn(100)))
				win.reference = bet.ext
				jobs = append(jobs, job{win})
			case 1:
				rf := newOp(w, "REFUND", amount)
				rf.reference = bet.ext
				jobs = append(jobs, job{rf}, job{rf})
				rb := newOp(w, "ROLLBACK", amount)
				rb.reference = bet.ext
				jobs = append(jobs, job{rb})
			case 2:
				jobs = append(jobs, job{newOp(w, "LOSS", "0.00")})
			case 3:
				jobs = append(jobs, job{bet})
			}
		}
	}
	rng.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })

	var bad sync.Map
	stopWatch := make(chan struct{})
	go func() {
		select {
		case <-time.After(8 * time.Second):
			rows, err := s.pool.Query(context.Background(), `SELECT pid, state, wait_event_type, wait_event, now()-xact_start, left(query, 160) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle' ORDER BY xact_start LIMIT 30`)
			if err == nil {
				for rows.Next() {
					var pid int
					var state, wt, we, q *string
					var age *string
					_ = rows.Scan(&pid, &state, &wt, &we, &age, &q)
					t.Logf("STALL pid=%d state=%v wait=%v/%v age=%v q=%v", pid, deref(state), deref(wt), deref(we), deref(age), deref(q))
				}
				rows.Close()
			}
		case <-stopWatch:
		}
	}()
	defer close(stopWatch)
	fanOut(len(jobs), func(i int) {
		r := submitRetry(t, insts[i%3].base, jobs[i].o)
		switch r.status {
		case http.StatusOK, http.StatusAccepted, http.StatusUnprocessableEntity:
		default:
			bad.Store(i, fmt.Sprintf("%d %s", r.status, r.body))
		}
	})
	bad.Range(func(k, v any) bool { t.Errorf("job %v: %v", k, v); return true })

	eventually(t, 30*time.Second, "no operation left pending", func() bool {
		return s.queryInt(`SELECT count(*) FROM wager_transactions WHERE status IN ('PENDING','PENDING_REFERENCE')`) == 0
	})

	for _, w := range ws {
		if b := s.balanceMinor(w.id); b < 0 {
			t.Fatalf("negative balance %d", b)
		}
		s.assertConsistent(w.id)
		var brokenChain int64
		if err := s.pool.QueryRow(context.Background(), `
			SELECT count(*) FROM (
			  SELECT wallet_version, balance_before_minor,
			         lag(balance_after_minor) OVER (ORDER BY wallet_version) AS prev_after,
			         lag(wallet_version)      OVER (ORDER BY wallet_version) AS prev_version
			    FROM wallet_ledger_entries WHERE wallet_id = $1) e
			 WHERE (prev_version IS NOT NULL AND (wallet_version <> prev_version + 1 OR balance_before_minor <> prev_after))`, w.id).Scan(&brokenChain); err != nil {
			t.Fatal(err)
		}
		if brokenChain != 0 {
			t.Fatalf("wallet %s has %d broken ledger links", w.id, brokenChain)
		}
	}
	if n := s.queryInt(`SELECT count(*) FROM (SELECT reference_transaction_id FROM wager_transactions
		WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK') GROUP BY 1 HAVING count(*) > 1) x`); n != 0 {
		t.Fatalf("%d transactions were reversed more than once", n)
	}
	if n := s.queryInt(`SELECT count(*) FROM wager_transactions t
		WHERE t.status='PROCESSED' AND t.kind <> 'LOSS' AND t.origin='EXTERNAL'
		  AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l WHERE l.transaction_id = t.id)`); n != 0 {
		t.Fatalf("%d processed transactions without ledger entry", n)
	}
	if n := s.queryInt(`SELECT count(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id
		WHERE t.status <> 'PROCESSED' OR t.kind = 'LOSS'`); n != 0 {
		t.Fatalf("%d ledger entries for rejected/LOSS transactions", n)
	}
	for _, w := range ws {
		r := call(t, http.MethodPost, insts[0].base+"/wallets/"+w.id+"/reconciliation", token(t, "wallet-service"), nil, nil)
		if r.json(t)["consistent"] != true {
			t.Fatalf("reconciliation of %s: %s", w.id, r.body)
		}
	}
	_ = uuid.Nil
}

func deref(p *string) string {
	if p == nil {
		return "-"
	}
	return *p
}
