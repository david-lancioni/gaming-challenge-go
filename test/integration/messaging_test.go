//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
)

func (s *stack) txStatus(ext string) string {
	var st string
	err := s.pool.QueryRow(context.Background(),
		`SELECT status FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id=$1`, ext).Scan(&st)
	if err != nil {
		return ""
	}
	return st
}

func (s *stack) awaitStatus(t testing.TB, ext, want string) {
	t.Helper()
	eventually(t, 30*time.Second, fmt.Sprintf("%s to become %s", ext, want), func() bool { return s.txStatus(ext) == want })
}

func (s *stack) debits(walletID string) int64 {
	return s.queryInt(`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND direction='DEBIT'`, walletID)
}

func (s *stack) queueDepth(t testing.TB, url string) int {
	t.Helper()
	out, err := s.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		}})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, v := range out.Attributes {
		var x int
		fmt.Sscan(v, &x)
		n += x
	}
	return n
}

func metricSum(t testing.TB, inst *instance, name string, fragments ...string) float64 {
	t.Helper()
	body := string(call(t, http.MethodGet, inst.metrics+"/metrics", "", nil, nil).body)
	total := 0.0
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		ok := true
		for _, f := range fragments {
			if !strings.Contains(line, f) {
				ok = false
			}
		}
		if ok {
			var v float64
			fmt.Sscan(line[strings.LastIndex(line, " ")+1:], &v)
			total += v
		}
	}
	return total
}

func TestSQSConsumerProcessesAndDeduplicates(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	bet := newOp(w, "BET", "10.00")

	s.sendRequest(t, "msg-1", bet)
	s.awaitStatus(t, bet.ext, "PROCESSED")
	if s.balanceMinor(w.id) != 9000 {
		t.Fatalf("balance = %d", s.balanceMinor(w.id))
	}

	for i := 0; i < 3; i++ {
		s.sendRaw(t, requestMessage("msg-1", bet), w.id, "redelivery-"+uuid.NewString())
	}
	eventually(t, 20*time.Second, "redeliveries to be acknowledged", func() bool {
		return metricSum(t, inst, "wagering_sqs_messages_total", `outcome="duplicate"`) >= 3
	})
	if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 9000 {
		t.Fatalf("redelivery changed the outcome: debits=%d balance=%d", s.debits(w.id), s.balanceMinor(w.id))
	}
	if n := s.queryInt(`SELECT count(*) FROM inbox_messages WHERE message_id='msg-1' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}

	s.sendRequest(t, "msg-2", bet)
	eventually(t, 20*time.Second, "second message id to be processed as a replay", func() bool {
		return s.queryInt(`SELECT count(*) FROM inbox_messages WHERE message_id='msg-2'`) == 1
	})
	if s.debits(w.id) != 1 {
		t.Fatalf("debits = %d", s.debits(w.id))
	}

	r := submit(t, inst.base, bet)
	wantStatus(t, r, http.StatusOK)
	if r.json(t)["idempotentReplay"] != true || amountOf(t, r, "balance") != "90.00" {
		t.Fatalf("http replay = %s", r.body)
	}

	changed := bet
	changed.amount = "55.00"
	s.sendRaw(t, requestMessage("msg-1", changed), w.id, "conflict-"+uuid.NewString())
	s.sendRequest(t, "msg-3", changed)
	dl := s.drain(t, s.urls.InputDLQ, 40*time.Second, func(m []messageT) bool { return len(m) >= 2 })
	if len(dl) != 2 {
		t.Fatalf("dead letters = %d", len(dl))
	}
	reasons := map[string]bool{}
	for _, m := range dl {
		reasons[aws.ToString(m.MessageAttributes["dlqReason"].StringValue)] = true
	}
	if !reasons["message_id_conflict"] || !reasons["idempotency_conflict"] {
		t.Fatalf("reasons = %v", reasons)
	}
	if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 9000 {
		t.Fatal("dead-lettered messages must have no financial effect")
	}
	eventually(t, 10*time.Second, "input queue to be empty", func() bool { return s.queueDepth(t, s.urls.Input) == 0 })
	s.assertConsistent(w.id)
}

func TestSQSInvalidMessagesGoToTheDeadLetterQueue(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	good := newOp(w, "BET", "1.00")

	badMoney := requestMessage("bad-money", good)
	badMoney = strings.Replace(badMoney, `"amount":"1.00"`, `"amount":"1.005"`, 1)
	opening := newOp(w, "OPENING", "5.00")
	loss := newOp(w, "LOSS", "5.00")
	cases := map[string]string{
		"not json":        "{{{",
		"empty object":    "{}",
		"unknown type":    strings.Replace(requestMessage("unknown-type", good), "WagerTransactionRequested", "SomethingElse", 1),
		"bad amount":      badMoney,
		"float amount":    strings.Replace(requestMessage("float", good), `"amount":"1.00"`, `"amount":1.0`, 1),
		"opening kind":    requestMessage("opening", opening),
		"loss with value": requestMessage("loss", loss),
		"extra field":     strings.Replace(requestMessage("extra", good), `"kind"`, `"unexpected":1,"kind"`, 1),
		"bad occurredAt":  strings.Replace(requestMessage("occ", good), `"occurredAt":"`, `"occurredAt":"yesterday`, 1),
	}
	for name, body := range cases {
		s.sendRaw(t, body, w.id, "dedup-"+strings.ReplaceAll(name, " ", "-")+uuid.NewString())
	}
	dl := s.drain(t, s.urls.InputDLQ, 40*time.Second, func(m []messageT) bool { return len(m) >= len(cases) })
	if len(dl) != len(cases) {
		t.Fatalf("dead letters = %d, want %d", len(dl), len(cases))
	}
	for _, m := range dl {
		if aws.ToString(m.MessageAttributes["dlqReason"].StringValue) != "invalid_message" {
			t.Errorf("reason = %v", m.MessageAttributes["dlqReason"])
		}
	}
	if n := s.queryInt(`SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND kind <> 'OPENING'`, w.id); n != 0 {
		t.Fatalf("invalid messages created %d transactions", n)
	}
	if s.balanceMinor(w.id) != 10000 {
		t.Fatal("balance changed")
	}
	if metricSum(t, inst, "wagering_dlq_messages_total", `reason="invalid_message"`) < float64(len(cases)) {
		t.Error("dlq metric missing")
	}
}

func TestSQSTransientFailuresAreRetriedWithBackoffThenDeadLettered(t *testing.T) {
	s, inst := single(t, map[string]string{"SQS_MAX_RECEIVE_COUNT": "2"})

	ghost := walletRef{id: uuid.NewString(), player: uuid.NewString()}
	o := newOp(ghost, "BET", "5.00")
	start := time.Now()
	s.sendRequest(t, "ghost-1", o)
	dl := s.drain(t, s.urls.InputDLQ, 90*time.Second, func(m []messageT) bool { return len(m) >= 1 })
	if len(dl) != 1 {
		t.Fatalf("dead letters = %d", len(dl))
	}
	if r := aws.ToString(dl[0].MessageAttributes["dlqReason"].StringValue); r != "retries_exhausted" {
		t.Errorf("reason = %s", r)
	}
	if time.Since(start) < 2*time.Second {
		t.Errorf("no backoff between attempts (took %s)", time.Since(start))
	}
	if metricSum(t, inst, "wagering_retries_total", `component="sqs_consumer"`) < 1 {
		t.Error("retry metric missing")
	}
}

func TestSQSRecoversFromTransientDatabaseUnavailability(t *testing.T) {
	s, inst := single(t, map[string]string{"DB_LOCK_TIMEOUT": "1s"})
	w := openWallet(t, inst.base, "100.00")
	bet := newOp(w, "BET", "10.00")

	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE`, w.id); err != nil {
		t.Fatal(err)
	}
	s.sendRequest(t, "locked-1", bet)
	eventually(t, 15*time.Second, "a transient failure to be observed", func() bool {
		return metricSum(t, inst, "wagering_retries_total", `component="sqs_consumer"`) >= 1
	})
	if s.txStatus(bet.ext) != "" {
		t.Fatal("nothing may be persisted while the attempt keeps failing")
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.awaitStatus(t, bet.ext, "PROCESSED")
	if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 9000 {
		t.Fatalf("debits=%d balance=%d", s.debits(w.id), s.balanceMinor(w.id))
	}
	if metricSum(t, inst, "wagering_concurrency_conflicts_total", `reason="lock_timeout"`) < 1 {
		t.Error("lock timeout conflict metric missing")
	}
}

func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	s := newStack(t)
	setup := s.startProcess(t, "setup", map[string]string{"ENABLE_CONSUMER": "false"})
	w := openWallet(t, setup.base, "100.00")
	bet := newOp(w, "BET", "30.00")

	crashing := s.spawn(t, "crashing", map[string]string{
		"ENABLE_HTTP": "false", "ENABLE_PUBLISHER": "false", "ENABLE_PENDING_WORKER": "false",
		"FAULT_INJECTION": "consumer_after_commit"})
	time.Sleep(3 * time.Second)
	s.sendRequest(t, "crash-1", bet)
	crashing.waitExit(t, 30*time.Second)

	if s.txStatus(bet.ext) != "PROCESSED" || s.debits(w.id) != 1 {
		t.Fatalf("expected the committed debit; status=%q debits=%d", s.txStatus(bet.ext), s.debits(w.id))
	}
	if n := s.queryInt(`SELECT count(*) FROM inbox_messages WHERE message_id='crash-1' AND completed_at IS NOT NULL`); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	if s.queueDepth(t, s.urls.Input) != 1 {
		t.Fatal("the message must still be in the queue (never deleted)")
	}

	s.startProcess(t, "survivor", map[string]string{"ENABLE_PUBLISHER": "false"})
	eventually(t, 40*time.Second, "redelivered message to be acknowledged", func() bool { return s.queueDepth(t, s.urls.Input) == 0 })
	if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 7000 {
		t.Fatalf("redelivery re-applied the operation: debits=%d balance=%d", s.debits(w.id), s.balanceMinor(w.id))
	}
	if n := s.queryInt(`SELECT count(*) FROM inbox_messages WHERE message_id='crash-1'`); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	s.assertConsistent(w.id)
}

func TestSameOperationThroughHTTPAndSQSConcurrently(t *testing.T) {
	s, insts := cluster(t, 3, nil)
	for round := 0; round < 5; round++ {
		w := openWallet(t, insts[0].base, "100.00")
		bet := newOp(w, "BET", "60.00")
		fanOut(8, func(i int) {
			if i%2 == 0 {
				s.sendRaw(t, requestMessage(fmt.Sprintf("x-%d-%d", round, i), bet), w.id, uuid.NewString())
			} else {
				submitRetry(t, insts[i%3].base, bet)
			}
		})
		s.awaitStatus(t, bet.ext, "PROCESSED")
		eventually(t, 30*time.Second, "all messages to be handled", func() bool {
			return s.queryInt(`SELECT count(*) FROM inbox_messages WHERE message_id LIKE $1`, fmt.Sprintf("x-%d-%%", round)) == 4
		})
		if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 4000 {
			t.Fatalf("round %d: debits=%d balance=%d", round, s.debits(w.id), s.balanceMinor(w.id))
		}
		s.assertConsistent(w.id)
	}
	w := openWallet(t, insts[0].base, "100.00")
	bet := newOp(w, "BET", "10.00")
	wantStatus(t, submit(t, insts[0].base, bet), http.StatusOK)
	other := bet
	other.key = "different-key"
	s.sendRequest(t, "cross-key", other)
	dl := s.drain(t, s.urls.InputDLQ, 30*time.Second, func(m []messageT) bool {
		for _, x := range m {
			if strings.Contains(aws.ToString(x.Body), "cross-key") {
				return true
			}
		}
		return false
	})
	if len(dl) == 0 || s.debits(w.id) != 1 {
		t.Fatalf("cross-key message: dl=%d debits=%d", len(dl), s.debits(w.id))
	}
}

func TestReversalBeforeReferenceThroughSQS(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "100.00")
	bet := newOp(w, "BET", "20.00")
	rollback := newOp(w, "ROLLBACK", "20.00")
	rollback.reference = bet.ext

	s.sendRequest(t, "rb-1", rollback)
	s.awaitStatus(t, rollback.ext, "PENDING_REFERENCE")
	eventually(t, 10*time.Second, "message to be acknowledged", func() bool { return s.queueDepth(t, s.urls.Input) == 0 })

	s.sendRequest(t, "bet-1", bet)
	s.awaitStatus(t, rollback.ext, "PROCESSED")
	if s.balanceMinor(w.id) != 10000 {
		t.Fatalf("balance = %d", s.balanceMinor(w.id))
	}
	s.assertConsistent(w.id)
}

func TestOutboxEventsHaveTheDocumentedEnvelopeAndOrderOfCauses(t *testing.T) {
	s, inst := single(t, nil)
	w := openWallet(t, inst.base, "50.00")
	bet := newOp(w, "BET", "20.00")
	wantStatus(t, submit(t, inst.base, bet), http.StatusOK)
	wantStatus(t, submit(t, inst.base, newOp(w, "LOSS", "0.00")), http.StatusOK)
	wantStatus(t, submit(t, inst.base, newOp(w, "BET", "500.00")), http.StatusUnprocessableEntity)
	pend := newOp(w, "REFUND", "1.00")
	pend.reference = "later"
	wantStatus(t, submit(t, inst.base, pend), http.StatusAccepted)

	msgs := s.drain(t, s.urls.Events, 30*time.Second, func(m []messageT) bool {
		return len(eventsOf(parseEvents(t, m), w.id)) >= 7
	})
	evs := eventsOf(parseEvents(t, msgs), w.id)
	if len(evs) != 7 {
		t.Fatalf("events = %d, want 7", len(evs))
	}
	if countType(evs, "WagerTransactionProcessed") != 3 || countType(evs, "WalletBalanceChanged") != 2 ||
		countType(evs, "WagerTransactionRejected") != 1 || countType(evs, "WagerTransactionPendingReference") != 1 {
		t.Fatalf("event mix wrong: %+v", evs)
	}
	seen := map[string]bool{}
	for _, e := range evs {
		if e.EventID == "" || seen[e.EventID] || e.AggregateID == "" || e.CorrelationID == "" || e.Version != 1 {
			t.Errorf("bad envelope: %+v", e)
		}
		seen[e.EventID] = true
		if _, err := time.Parse(time.RFC3339, e.OccurredAt); err != nil || !strings.HasSuffix(e.OccurredAt, "Z") {
			t.Errorf("occurredAt %q must be UTC RFC 3339", e.OccurredAt)
		}
		if e.EventType == "WalletBalanceChanged" {
			for _, k := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
				if _, ok := e.Data[k]; !ok {
					t.Errorf("WalletBalanceChanged missing %s: %+v", k, e)
				}
			}
			if e.AggregateID != w.id {
				t.Errorf("WalletBalanceChanged aggregate = %s", e.AggregateID)
			}
		}
	}
	for id := range seen {
		if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE id=$1 AND published_at IS NOT NULL`, id); n != 1 {
			t.Errorf("event %s not marked as published", id)
		}
	}
	if n := s.queryInt(`SELECT count(*) FROM outbox_events o WHERE o.partition_key=$1 AND o.event_type <> 'WalletBalanceChanged'
		AND NOT EXISTS (SELECT 1 FROM wager_transactions t WHERE t.id = o.aggregate_id)`, w.id); n != 0 {
		t.Errorf("%d events without a committed transaction", n)
	}
	before := s.queryInt(`SELECT count(*) FROM outbox_events`)
	bad := newOp(w, "BET", "0.00")
	wantStatus(t, submit(t, inst.base, bad), http.StatusBadRequest)
	if s.queryInt(`SELECT count(*) FROM outbox_events`) != before {
		t.Error("an invalid request produced outbox events")
	}
}

func createBacklog(t *testing.T, s *stack, wallets int) []walletRef {
	t.Helper()
	feeder := s.startProcess(t, "feeder", map[string]string{"ENABLE_PUBLISHER": "false", "ENABLE_CONSUMER": "false", "ENABLE_PENDING_WORKER": "false"})
	ws := make([]walletRef, wallets)
	for i := range ws {
		ws[i] = openWallet(t, feeder.base, "100.00")
		wantStatus(t, submit(t, feeder.base, newOp(ws[i], "BET", "1.00")), http.StatusOK)
	}
	feeder.kill()
	return ws
}

func (s *stack) unpublished() int64 {
	return s.queryInt(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
}

func TestTwoPublishersCompeteForTheSameOutbox(t *testing.T) {
	s := newStack(t)
	ws := createBacklog(t, s, 20)
	total := s.unpublished()
	if total != 80 {
		t.Fatalf("backlog = %d", total)
	}
	for i := 0; i < 3; i++ {
		s.spawn(t, fmt.Sprintf("publisher-%d", i), map[string]string{
			"ENABLE_HTTP": "false", "ENABLE_CONSUMER": "false", "ENABLE_PENDING_WORKER": "false",
			"OUTBOX_BATCH_SIZE": "5",
			"OUTBOX_LEASE":      "30m"})
	}
	eventually(t, 60*time.Second, "outbox to drain", func() bool { return s.unpublished() == 0 })

	if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE attempts <> 1`); n != 0 {
		rows, _ := s.pool.Query(context.Background(), `SELECT id::text, event_type, attempts, coalesce(last_error,'-'), coalesce(locked_by,'-') FROM outbox_events WHERE attempts <> 1`)
		for rows.Next() {
			var id, typ, lastErr, by string
			var attempts int
			_ = rows.Scan(&id, &typ, &attempts, &lastErr, &by)
			t.Logf("event %s %s attempts=%d last_error=%s locked_by=%s", id, typ, attempts, lastErr, by)
		}
		t.Fatalf("%d events were claimed more than once by competing publishers", n)
	}
	msgs := s.drain(t, s.urls.Events, 30*time.Second, func(m []messageT) bool { return int64(len(m)) >= total })
	ids := map[string]bool{}
	for _, e := range parseEvents(t, msgs) {
		ids[e.EventID] = true
	}
	if int64(len(ids)) != total {
		t.Fatalf("distinct events delivered = %d, want %d", len(ids), total)
	}
	_ = ws
}

func TestPublisherCrashBetweenPublishAndConfirmation(t *testing.T) {
	s := newStack(t)
	createBacklog(t, s, 4)
	total := s.unpublished()

	crashing := s.spawn(t, "crashing-publisher", map[string]string{
		"ENABLE_HTTP": "false", "ENABLE_CONSUMER": "false", "ENABLE_PENDING_WORKER": "false",
		"FAULT_INJECTION": "publisher_after_send", "OUTBOX_BATCH_SIZE": "1"})
	crashing.waitExit(t, 30*time.Second)
	if s.unpublished() != total {
		t.Fatalf("unpublished = %d, want %d", s.unpublished(), total)
	}
	var crashedID string
	crashedID = s.queryString(`SELECT id::text FROM outbox_events WHERE locked_by IS NOT NULL AND published_at IS NULL LIMIT 1`)

	s.spawn(t, "takeover", map[string]string{"ENABLE_HTTP": "false", "ENABLE_CONSUMER": "false", "ENABLE_PENDING_WORKER": "false"})
	eventually(t, 60*time.Second, "another instance to publish everything", func() bool { return s.unpublished() == 0 })

	if n := s.queryInt(`SELECT attempts FROM outbox_events WHERE id=$1`, crashedID); n < 2 {
		t.Errorf("the abandoned event was claimed %d time(s), want a re-claim", n)
	}
	msgs := s.drain(t, s.urls.Events, 15*time.Second, func(m []messageT) bool { return int64(len(m)) >= total })
	ids := map[string]bool{}
	for _, e := range parseEvents(t, msgs) {
		ids[e.EventID] = true
	}
	if !ids[crashedID] {
		t.Fatalf("the event %s that was being published at the crash was not delivered", crashedID)
	}
	if int64(len(ids)) != total {
		t.Fatalf("distinct events delivered = %d, want %d", len(ids), total)
	}
}

func TestOutboxSurvivesSQSOutage(t *testing.T) {
	s, inst := single(t, map[string]string{"ENABLE_CONSUMER": "false"})
	w := openWallet(t, inst.base, "100.00")
	eventually(t, 20*time.Second, "initial events published", func() bool { return s.unpublished() == 0 })
	_ = s.drain(t, s.urls.Events, 3*time.Second, nil)

	if _, err := s.sqs.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(s.urls.Events)}); err != nil {
		t.Skipf("cannot simulate an outage on this emulator: %v", err)
	}
	for i := 0; i < 3; i++ {
		wantStatus(t, submit(t, inst.base, newOp(w, "BET", "1.00")), http.StatusOK)
	}
	eventually(t, 20*time.Second, "publication failures to be recorded", func() bool {
		return metricSum(t, inst, "wagering_outbox_publish_failures_total") >= 1
	})
	if s.unpublished() != 6 {
		t.Fatalf("unpublished during the outage = %d, want 6", s.unpublished())
	}
	if n := s.queryInt(`SELECT count(*) FROM outbox_events WHERE last_error IS NOT NULL AND published_at IS NULL`); n == 0 {
		t.Error("failures must be recorded on the events")
	}

	urls, err := recreateQueues(s)
	if err != nil {
		t.Fatalf("recreate: %v", err)
	}
	eventually(t, 60*time.Second, "backlog to be published after recovery", func() bool { return s.unpublished() == 0 })
	msgs := s.drain(t, urls, 15*time.Second, func(m []messageT) bool { return len(m) >= 6 })
	if got := len(eventsOf(parseEvents(t, msgs), w.id)); got != 6 {
		t.Fatalf("delivered %d events after recovery, want 6", got)
	}
}

func TestOutboxClaimKeepsPerWalletOrder(t *testing.T) {
	s := newStack(t)
	ctx := context.Background()
	q := postgres.NewOutboxQueue(s.pool)
	walletA, walletB := uuid.NewString(), uuid.NewString()
	insert := func(wallet string) string {
		id := uuid.NewString()
		if err := s.exec(`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, partition_key, event_type, event_version, payload, occurred_at)
			VALUES ($1, 'Wallet', $2, $2, 'WalletBalanceChanged', 1, '{}', now())`, id, wallet); err != nil {
			t.Fatal(err)
		}
		return id
	}
	a1, a2, a3, b1 := insert(walletA), insert(walletA), insert(walletA), insert(walletB)
	ids := func(recs []application.OutboxRecord) map[string]bool {
		m := map[string]bool{}
		for _, r := range recs {
			m[r.ID.String()] = true
		}
		return m
	}

	first, err := q.Claim(ctx, "p1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(first); len(got) != 2 || !got[a1] || !got[b1] {
		t.Fatalf("first claim must return only the head of each wallet, got %v", got)
	}
	other, err := q.Claim(ctx, "p2", 10, time.Minute)
	if err != nil || len(other) != 0 {
		t.Fatalf("a concurrent publisher must not receive a2 before a1 is published: %v %v", other, err)
	}
	id := func(s string) uuid.UUID { return uuid.MustParse(s) }
	if err := q.MarkPublished(ctx, id(a1)); err != nil {
		t.Fatal(err)
	}
	next, _ := q.Claim(ctx, "p2", 10, time.Minute)
	if got := ids(next); len(got) != 1 || !got[a2] {
		t.Fatalf("after a1, only a2 may be claimed, got %v", got)
	}
	if err := q.MarkFailed(ctx, id(a2), "sqs unavailable", time.Hour); err != nil {
		t.Fatal(err)
	}
	blocked, _ := q.Claim(ctx, "p3", 10, time.Minute)
	if len(blocked) != 0 {
		t.Fatalf("a3 overtook the failed a2: %v", ids(blocked))
	}
	if err := q.MarkPublished(ctx, id(b1)); err != nil {
		t.Fatal(err)
	}
	if err := s.exec(`UPDATE outbox_events SET next_attempt_at = now() WHERE id = $1`, a2); err != nil {
		t.Fatal(err)
	}
	retry, _ := q.Claim(ctx, "p3", 10, time.Minute)
	if got := ids(retry); len(got) != 1 || !got[a2] {
		t.Fatalf("the retry of a2 must come before a3, got %v", got)
	}
	_ = a3
}
