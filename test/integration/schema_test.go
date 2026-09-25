//go:build integration

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
	"github.com/dlancioni/backend-challenge-go/migrations"
)

func TestMigrationsApplyAndRevert(t *testing.T) {
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDBURL)
	if err != nil {
		t.Fatal(err)
	}
	name := "it_mig_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})
	url := replaceDB(adminDBURL, name)

	m, err := postgres.NewMigrator(ctx, url, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(ctx)

	applied, err := m.Up(ctx)
	if err != nil || len(applied) != 1 || applied[0] != 1 {
		t.Fatalf("up = %v, %v", applied, err)
	}
	if again, err := m.Up(ctx); err != nil || len(again) != 0 {
		t.Fatalf("second up must be a no-op: %v, %v", again, err)
	}
	tables := func() int {
		conn, _ := pgx.Connect(ctx, url)
		defer conn.Close(ctx)
		var n int
		_ = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public'
			AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','outbox_events','inbox_messages')`).Scan(&n)
		return n
	}
	if tables() != 5 {
		t.Fatalf("tables after up = %d", tables())
	}
	reverted, err := m.Down(ctx, 1)
	if err != nil || len(reverted) != 1 {
		t.Fatalf("down = %v, %v", reverted, err)
	}
	if tables() != 0 {
		t.Fatalf("tables after down = %d", tables())
	}
	if list, _ := m.Applied(ctx); len(list) != 0 {
		t.Fatalf("applied after down = %v", list)
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatalf("up after down: %v", err)
	}
	if tables() != 5 {
		t.Fatal("re-applied schema is incomplete")
	}
}

func expectDB(t *testing.T, err error, code, want string) {
	t.Helper()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		t.Fatalf("expected a database error (%s %s), got %v", code, want, err)
	}
	if pe.Code != code || !(strings.Contains(pe.ConstraintName, want) || strings.Contains(pe.Message, want)) {
		t.Fatalf("got %s %q (constraint %q), want %s containing %q", pe.Code, pe.Message, pe.ConstraintName, code, want)
	}
}

type fixture struct {
	s      *stack
	wallet string
	player string
}

func newFixture(t *testing.T, s *stack) fixture {
	t.Helper()
	f := fixture{s: s, wallet: uuid.NewString(), player: uuid.NewString()}
	if err := s.exec(`INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1,$2,'BRL',0,1,now(),now())`, f.wallet, f.player); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f fixture) insertTx(q interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, id, kind, status string, amount int64, ext string) error {
	balance := "NULL"
	failure := "NULL"
	switch status {
	case "PROCESSED":
		balance = "0"
	case "REJECTED":
		failure = "'X'"
	}
	_, err := q.Exec(context.Background(), `INSERT INTO wager_transactions
		(id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
		 kind, amount_minor, currency, status, failure_code, result_balance_minor, created_at, updated_at)
		VALUES ($1,'EXTERNAL','provider-a',$2,$3,'h',$4,$5,'r','g',$6,$7,'BRL',$8,`+failure+`,`+balance+`,now(),now())`,
		id, ext, "key-"+ext, f.wallet, f.player, kind, amount, status)
	return err
}

func (f fixture) credit(t *testing.T, amount int64) (txID string) {
	t.Helper()
	ctx := context.Background()
	tx, err := f.s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	txID = uuid.NewString()
	if err := f.insertTx(tx, txID, "WIN", "PROCESSED", amount, txID); err != nil {
		t.Fatal(err)
	}
	var version, balance int64
	if err := tx.QueryRow(ctx, `SELECT version, balance_minor FROM wallets WHERE id=$1 FOR UPDATE`, f.wallet).Scan(&version, &balance); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'CREDIT',$4,'BRL',$5,$6,$7,now())`,
		uuid.NewString(), f.wallet, txID, amount, balance, balance+amount, version+1); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor=$2, version=$3, updated_at=now() WHERE id=$1`, f.wallet, balance+amount, version+1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("valid credit must commit: %v", err)
	}
	return txID
}

func TestSchemaEnforcesWalletInvariants(t *testing.T) {
	s := newStack(t)
	f := newFixture(t, s)
	ctx := context.Background()

	t.Run("balance cannot be negative", func(t *testing.T) {
		err := s.exec(`INSERT INTO wallets VALUES ($1,$2,'BRL',-1,1,now(),now())`, uuid.NewString(), uuid.NewString())
		expectDB(t, err, "23514", "wallets_balance_non_negative")
	})
	t.Run("version starts at one", func(t *testing.T) {
		err := s.exec(`INSERT INTO wallets VALUES ($1,$2,'BRL',0,0,now(),now())`, uuid.NewString(), uuid.NewString())
		expectDB(t, err, "23514", "wallets_version_positive")
	})
	t.Run("one wallet per player and currency", func(t *testing.T) {
		err := s.exec(`INSERT INTO wallets VALUES ($1,$2,'BRL',0,1,now(),now())`, uuid.NewString(), f.player)
		expectDB(t, err, "23505", "wallets_player_currency_key")
		if err := s.exec(`INSERT INTO wallets VALUES ($1,$2,'USD',0,1,now(),now())`, uuid.NewString(), f.player); err != nil {
			t.Fatalf("another currency is a different wallet: %v", err)
		}
	})
	t.Run("balance cannot change without a ledger entry", func(t *testing.T) {
		tx, _ := s.pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor=500, version=2 WHERE id=$1`, f.wallet); err != nil {
			t.Fatalf("the update itself is accepted until commit: %v", err)
		}
		expectDB(t, tx.Commit(ctx), "23000", "ledger")
		if s.balanceMinor(f.wallet) != 0 {
			t.Fatal("the balance changed")
		}
	})
	t.Run("balance change requires a version increment by one", func(t *testing.T) {
		expectDB(t, s.exec(`UPDATE wallets SET balance_minor=500 WHERE id=$1`, f.wallet), "23000", "without a version increment")
		expectDB(t, s.exec(`UPDATE wallets SET version=9 WHERE id=$1`, f.wallet), "23000", "without a balance change")
	})
	t.Run("a wallet is created with the opening state only", func(t *testing.T) {
		tx, _ := s.pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO wallets VALUES ($1,$2,'BRL',900,1,now(),now())`, uuid.NewString(), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
		expectDB(t, tx.Commit(ctx), "23000", "has no ledger entries")
	})
	t.Run("identity is immutable and wallets cannot be deleted", func(t *testing.T) {
		expectDB(t, s.exec(`UPDATE wallets SET player_id=$2 WHERE id=$1`, f.wallet, uuid.NewString()), "23000", "identity")
		expectDB(t, s.exec(`UPDATE wallets SET currency='USD' WHERE id=$1`, f.wallet), "23000", "identity")
		expectDB(t, s.exec(`DELETE FROM wallets WHERE id=$1`, f.wallet), "23000", "cannot be deleted")
	})
	t.Run("valid movement commits", func(t *testing.T) {
		f.credit(t, 1234)
		if s.balanceMinor(f.wallet) != 1234 || s.queryInt(`SELECT version FROM wallets WHERE id=$1`, f.wallet) != 2 {
			t.Fatal("unexpected wallet state")
		}
	})
}

func TestSchemaLedgerIsAppendOnlyAndConsistent(t *testing.T) {
	s := newStack(t)
	f := newFixture(t, s)
	ctx := context.Background()
	winTx := f.credit(t, 1000)

	t.Run("no update, delete or truncate", func(t *testing.T) {
		expectDB(t, s.exec(`UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id=$1`, f.wallet), "23000", "append-only")
		expectDB(t, s.exec(`DELETE FROM wallet_ledger_entries WHERE wallet_id=$1`, f.wallet), "23000", "append-only")
		expectDB(t, s.exec(`TRUNCATE wallet_ledger_entries`), "23000", "append-only")
		if s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, f.wallet) != 1 {
			t.Fatal("ledger changed")
		}
	})
	t.Run("balanceAfter must equal balanceBefore plus or minus the amount", func(t *testing.T) {
		betTx := uuid.NewString()
		if err := f.insertTx(s.pool, betTx, "BET", "PROCESSED", 100, betTx); err != nil {
			t.Fatal(err)
		}
		err := s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',1000,950,3,now())`, uuid.NewString(), f.wallet, betTx)
		expectDB(t, err, "23514", "ledger_arithmetic")
		err = s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',1000,-100,3,now())`, uuid.NewString(), f.wallet, betTx)
		expectDB(t, err, "23514", "ledger_arithmetic")
		err = s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',1000,900,5,now())`, uuid.NewString(), f.wallet, betTx)
		expectDB(t, err, "23000", "breaks the balance chain")
		err = s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',999,899,3,now())`, uuid.NewString(), f.wallet, betTx)
		expectDB(t, err, "23000", "breaks the balance chain")
	})
	t.Run("one entry per wallet and transaction", func(t *testing.T) {
		err := s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'CREDIT',1000,'BRL',1000,2000,3,now())`, uuid.NewString(), f.wallet, winTx)
		expectDB(t, err, "23505", "ledger_wallet_transaction_key")
	})
	t.Run("LOSS and rejected transactions cannot have entries", func(t *testing.T) {
		loss := uuid.NewString()
		if err := f.insertTx(s.pool, loss, "LOSS", "PROCESSED", 0, loss); err != nil {
			t.Fatal(err)
		}
		err := s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'CREDIT',1,'BRL',1000,1001,3,now())`, uuid.NewString(), f.wallet, loss)
		if err == nil {
			t.Fatal("an entry for a LOSS must be refused")
		}
		rejected := uuid.NewString()
		if err := f.insertTx(s.pool, rejected, "BET", "REJECTED", 50, rejected); err != nil {
			t.Fatal(err)
		}
		err = s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',50,'BRL',1000,950,3,now())`, uuid.NewString(), f.wallet, rejected)
		expectDB(t, err, "23000", "processed transaction")
	})
	t.Run("a BET is always a debit and a WIN a credit", func(t *testing.T) {
		bet := uuid.NewString()
		if err := f.insertTx(s.pool, bet, "BET", "PROCESSED", 10, bet); err != nil {
			t.Fatal(err)
		}
		err := s.exec(`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'CREDIT',10,'BRL',1000,1010,3,now())`, uuid.NewString(), f.wallet, bet)
		expectDB(t, err, "23000", "must not produce")
	})
	_ = ctx
}

func TestSchemaTransactionRules(t *testing.T) {
	s := newStack(t)
	f := newFixture(t, s)
	id := uuid.NewString()
	if err := f.insertTx(s.pool, id, "BET", "PENDING", 500, "ext-1"); err != nil {
		t.Fatal(err)
	}

	t.Run("external id and idempotency key are unique per provider", func(t *testing.T) {
		err := f.insertTx(s.pool, uuid.NewString(), "BET", "PENDING", 1, "ext-1")
		expectDB(t, err, "23505", "wager_tx_provider_external_key")
		err = s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ($1,'EXTERNAL','provider-a','other-ext','key-ext-1','h',$2,$3,'r','g','BET',1,'BRL','PENDING',now(),now())`, uuid.NewString(), f.wallet, f.player)
		expectDB(t, err, "23505", "wager_tx_provider_idempotency_key")
		err = s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ($1,'EXTERNAL','provider-b','ext-1','key-ext-1','h',$2,$3,'r','g','BET',1,'BRL','PENDING',now(),now())`, uuid.NewString(), f.wallet, f.player)
		if err != nil {
			t.Fatalf("provider-b: %v", err)
		}
	})
	t.Run("amount policy per kind", func(t *testing.T) {
		expectDB(t, f.insertTx(s.pool, uuid.NewString(), "BET", "PENDING", 0, "z1"), "23514", "wager_tx_amount_policy")
		expectDB(t, f.insertTx(s.pool, uuid.NewString(), "LOSS", "PENDING", 5, "z2"), "23514", "wager_tx_amount_policy")
		expectDB(t, f.insertTx(s.pool, uuid.NewString(), "BET", "PENDING", -5, "z3"), "23514", "")
	})
	t.Run("reversals need a reference", func(t *testing.T) {
		expectDB(t, f.insertTx(s.pool, uuid.NewString(), "REFUND", "PENDING", 5, "z4"), "23514", "wager_tx_reversal_reference")
	})
	t.Run("internal and external origins have distinct shapes", func(t *testing.T) {
		err := s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, wallet_id, player_id, kind, amount_minor, currency, status, result_balance_minor, created_at, updated_at)
			VALUES ($1,'INTERNAL','provider-a',$2,$3,'OPENING',5,'BRL','PROCESSED',5,now(),now())`, uuid.NewString(), f.wallet, f.player)
		expectDB(t, err, "23514", "wager_tx_origin_shape")
		err = s.exec(`INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ($1,'EXTERNAL',$2,$3,'BET',5,'BRL','PENDING',now(),now())`, uuid.NewString(), f.wallet, f.player)
		expectDB(t, err, "23514", "wager_tx_origin_shape")
	})
	t.Run("a wallet has a single OPENING", func(t *testing.T) {
		ins := func() error {
			return s.exec(`INSERT INTO wager_transactions (id, origin, wallet_id, player_id, kind, amount_minor, currency, status, result_balance_minor, created_at, updated_at)
				VALUES ($1,'INTERNAL',$2,$3,'OPENING',5,'BRL','PROCESSED',5,now(),now())`, uuid.NewString(), f.wallet, f.player)
		}
		if err := ins(); err != nil {
			t.Fatal(err)
		}
		expectDB(t, ins(), "23505", "wager_tx_one_opening_per_wallet")
	})
	t.Run("a referenced transaction gets one successful reversal only", func(t *testing.T) {
		bet := uuid.NewString()
		if err := f.insertTx(s.pool, bet, "BET", "PROCESSED", 7, "ref-bet"); err != nil {
			t.Fatal(err)
		}
		reversal := func(kind, ext string) error {
			return s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
				kind, amount_minor, currency, reference_external_transaction_id, reference_transaction_id, status, result_balance_minor, created_at, updated_at)
				VALUES ($1,'EXTERNAL','provider-a',$2,$2,'h',$3,$4,'r','g',$5,7,'BRL','ref-bet',$6,'PROCESSED',0,now(),now())`, uuid.NewString(), ext, f.wallet, f.player, kind, bet)
		}
		if err := reversal("REFUND", "rev-1"); err != nil {
			t.Fatal(err)
		}
		expectDB(t, reversal("REFUND", "rev-2"), "23505", "wager_tx_one_reversal_per_reference")
		expectDB(t, reversal("ROLLBACK", "rev-3"), "23505", "wager_tx_one_reversal_per_reference")
	})
	t.Run("terminal states are final and rows are never deleted", func(t *testing.T) {
		if err := s.exec(`UPDATE wager_transactions SET status='PROCESSED', result_balance_minor=0, processed_at=now() WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
		expectDB(t, s.exec(`UPDATE wager_transactions SET status='REJECTED', failure_code='X' WHERE id=$1`, id), "23000", "terminal state")
		expectDB(t, s.exec(`UPDATE wager_transactions SET attempts=9 WHERE id=$1`, id), "23000", "terminal state")
		expectDB(t, s.exec(`DELETE FROM wager_transactions WHERE id=$1`, id), "23000", "cannot be deleted")
	})
	t.Run("business attributes are immutable while pending", func(t *testing.T) {
		p := uuid.NewString()
		if err := f.insertTx(s.pool, p, "BET", "PENDING", 3, "ext-imm"); err != nil {
			t.Fatal(err)
		}
		expectDB(t, s.exec(`UPDATE wager_transactions SET amount_minor=4 WHERE id=$1`, p), "23000", "immutable")
		expectDB(t, s.exec(`UPDATE wager_transactions SET payload_hash='x' WHERE id=$1`, p), "23000", "immutable")
		expectDB(t, s.exec(`UPDATE wager_transactions SET kind='WIN' WHERE id=$1`, p), "23000", "immutable")
	})
	t.Run("PROCESSED requires the result balance, REJECTED a failure code", func(t *testing.T) {
		expectDB(t, s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ($1,'EXTERNAL','provider-a','s1','s1','h',$2,$3,'r','g','BET',1,'BRL','PROCESSED',now(),now())`, uuid.NewString(), f.wallet, f.player), "23514", "wager_tx_status_shape")
		expectDB(t, s.exec(`INSERT INTO wager_transactions (id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status, created_at, updated_at)
			VALUES ($1,'EXTERNAL','provider-a','s2','s2','h',$2,$3,'r','g','BET',1,'BRL','REJECTED',now(),now())`, uuid.NewString(), f.wallet, f.player), "23514", "wager_tx_status_shape")
	})
}

func TestSchemaOutboxAndInbox(t *testing.T) {
	s := newStack(t)
	id := uuid.NewString()
	agg := uuid.NewString()
	if err := s.exec(`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, partition_key, event_type, event_version, payload, occurred_at)
		VALUES ($1,'Wallet',$2,$2,'WalletBalanceChanged',1,'{"a":1}',now())`, id, agg); err != nil {
		t.Fatal(err)
	}
	expectDB(t, s.exec(`UPDATE outbox_events SET payload='{"a":2}' WHERE id=$1`, id), "23000", "immutable")
	expectDB(t, s.exec(`UPDATE outbox_events SET event_type='X' WHERE id=$1`, id), "23000", "immutable")
	expectDB(t, s.exec(`DELETE FROM outbox_events WHERE id=$1`, id), "23000", "cannot be deleted")
	if err := s.exec(`UPDATE outbox_events SET attempts=attempts+1, last_error='e', next_attempt_at=now() WHERE id=$1`, id); err != nil {
		t.Fatalf("delivery bookkeeping must be updatable: %v", err)
	}
	if err := s.exec(`UPDATE outbox_events SET published_at=now() WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err := s.exec(`DELETE FROM outbox_events WHERE id=$1`, id); err != nil {
		t.Fatalf("published events may be purged: %v", err)
	}

	ins := func() error {
		return s.exec(`INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at) VALUES ('c','m','h',now())`)
	}
	if err := ins(); err != nil {
		t.Fatal(err)
	}
	expectDB(t, ins(), "23505", "inbox_messages_pkey")
}
