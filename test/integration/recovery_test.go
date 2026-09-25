//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
)

func TestRestartPreservesIdempotencyPendingAndConsistency(t *testing.T) {
	s := newStack(t)
	a := s.startProcess(t, "before-restart", nil)
	w := openWallet(t, a.base, "100.00")

	bet := newOp(w, "BET", "30.00")
	first := submit(t, a.base, bet)
	wantStatus(t, first, http.StatusOK)

	awaited := newOp(w, "BET", "10.00")
	refund := newOp(w, "REFUND", "10.00")
	refund.reference = awaited.ext
	pend := submit(t, a.base, refund)
	wantStatus(t, pend, http.StatusAccepted)

	a.kill()

	b := s.startProcess(t, "after-restart", nil)
	replay := submit(t, b.base, bet)
	wantStatus(t, replay, http.StatusOK)
	if replay.str(t, "transactionId") != first.str(t, "transactionId") || replay.json(t)["idempotentReplay"] != true ||
		amountOf(t, replay, "balance") != "70.00" {
		t.Fatalf("replay after restart = %s", replay.body)
	}
	if s.debits(w.id) != 1 {
		t.Fatalf("debits = %d", s.debits(w.id))
	}
	if s.txStatus(refund.ext) != "PENDING_REFERENCE" {
		t.Fatalf("pending state lost: %q", s.txStatus(refund.ext))
	}
	wantStatus(t, submit(t, b.base, awaited), http.StatusOK)
	s.awaitStatus(t, refund.ext, "PROCESSED")
	if s.balanceMinor(w.id) != 7000 {
		t.Fatalf("balance = %d", s.balanceMinor(w.id))
	}
	s.assertConsistent(w.id)
}

func TestCommittedPendingOperationsAreResumedByAnotherInstance(t *testing.T) {
	s := newStack(t)
	setup := s.startProcess(t, "setup", map[string]string{
		"ENABLE_PENDING_WORKER": "false", "ENABLE_CONSUMER": "false", "ENABLE_PUBLISHER": "false"})
	w := openWallet(t, setup.base, "100.00")
	setup.kill()

	uow := postgres.NewUnitOfWork(s.pool, nil)
	insertPending := func(o op, leaseFor time.Duration) *domain.WagerTransaction {
		tx, err := domain.NewExternalTransaction(domain.ExternalParams{
			ProviderID: o.provider, ExternalTransactionID: o.ext, IdempotencyKey: o.key, PlayerID: o.wallet.player,
			WalletID: o.wallet.id, RoundID: "round-1", GameID: "game-1", Kind: domain.TransactionKind(o.kind),
			Money: mustMoney(t, o.amount), CorrelationID: "pending-test",
		}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := uow.Do(context.Background(), func(ctx context.Context, r application.Repositories) error {
			return r.Transactions().Insert(ctx, tx)
		}); err != nil {
			t.Fatal(err)
		}
		if leaseFor > 0 {
			if err := s.exec(`UPDATE wager_transactions SET next_attempt_at = now() + $2::bigint * interval '1 millisecond' WHERE id = $1`,
				tx.ID(), leaseFor.Milliseconds()); err != nil {
				t.Fatal(err)
			}
		}
		return tx
	}
	due, leased := newOp(w, "BET", "20.00"), newOp(w, "BET", "5.00")
	due.key, leased.key = "provider-a:"+due.ext, "provider-a:"+leased.ext
	insertPending(due, 0)
	insertPending(leased, 4*time.Second)
	if s.txStatus(due.ext) != "PENDING" || s.balanceMinor(w.id) != 10000 {
		t.Fatal("precondition: pending and not applied")
	}

	rescuer := s.startProcess(t, "rescuer", nil)
	s.awaitStatus(t, due.ext, "PROCESSED")
	s.awaitStatus(t, leased.ext, "PROCESSED")
	if s.balanceMinor(w.id) != 7500 || s.debits(w.id) != 2 {
		t.Fatalf("balance=%d debits=%d", s.balanceMinor(w.id), s.debits(w.id))
	}
	s.assertConsistent(w.id)
	r := submit(t, rescuer.base, due)
	wantStatus(t, r, http.StatusOK)
	if r.json(t)["idempotentReplay"] != true || amountOf(t, r, "balance") != "80.00" {
		t.Fatalf("replay = %s", r.body)
	}
}

func mustMoney(t testing.TB, amount string) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}
