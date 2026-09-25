package messaging

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

const validBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func TestParseMessageAcceptsTheDocumentedEnvelope(t *testing.T) {
	msg, err := ParseMessage(validBody, now)
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != "msg-123" || msg.Params.IdempotencyKey != "provider-a:transaction-123" ||
		msg.Params.Kind != domain.KindBet || msg.Params.Money.Amount() != "25.00" || msg.Params.CausationID != "msg-123" {
		t.Fatalf("params = %+v", msg.Params)
	}
	if len(msg.InboxHash) != 64 {
		t.Fatalf("hash = %q", msg.InboxHash)
	}
	again, _ := ParseMessage(validBody, now.Add(time.Hour))
	if again.InboxHash != msg.InboxHash {
		t.Fatal("the hash must not depend on the reception time")
	}
	for name, mutated := range map[string]string{
		"amount": strings.Replace(validBody, `"25.00"`, `"25.01"`, 1),
		"key":    strings.Replace(validBody, `"provider-a:transaction-123"`, `"other"`, 1),
	} {
		got, err := ParseMessage(mutated, now)
		if err != nil || got.InboxHash == msg.InboxHash {
			t.Errorf("%s: err=%v same hash=%v", name, err, got.InboxHash == msg.InboxHash)
		}
	}
	other := strings.Replace(strings.Replace(validBody, "2026-09-08T12:00:00.000Z", "2027-01-01T00:00:00Z", 1), "msg-123", "msg-999", 1)
	if got, _ := ParseMessage(other, now); got.InboxHash != msg.InboxHash {
		t.Error("messageId and occurredAt are not part of the content hash")
	}
}

func TestParseMessageRejectsInvalidMessages(t *testing.T) {
	rep := func(old, new string) string { return strings.Replace(validBody, old, new, 1) }
	cases := map[string]string{
		"empty":            "",
		"not json":         "{{",
		"array":            "[]",
		"trailing data":    validBody + `{"x":1}`,
		"unknown type":     rep("WagerTransactionRequested", "Other"),
		"missing id":       rep(`"messageId": "msg-123",`, ""),
		"bad occurredAt":   rep("2026-09-08T12:00:00.000Z", "yesterday"),
		"unknown field":    rep(`"kind": "BET",`, `"kind": "BET", "extra": 1,`),
		"OPENING":          rep(`"BET"`, `"OPENING"`),
		"float amount":     rep(`"amount": "25.00"`, `"amount": 25.0`),
		"scale":            rep(`"25.00"`, `"25.001"`),
		"negative":         rep(`"25.00"`, `"-25.00"`),
		"scientific":       rep(`"25.00"`, `"2.5e1"`),
		"missing key":      rep(`"idempotencyKey": "provider-a:transaction-123",`, ""),
		"bad wallet":       rep("0192f291-27dd-7d3f-8071-5f8685deef37", "nope"),
		"reversal no ref":  rep(`"BET"`, `"REFUND"`),
		"BET with ref":     rep(`"kind": "BET",`, `"kind": "BET", "referenceExternalTransactionId": "x",`),
		"LOSS with amount": rep(`"BET"`, `"LOSS"`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseMessage(body, now); err == nil {
				t.Fatal("expected an error")
			} else if !isPermanent(err) {
				t.Fatalf("invalid messages must be permanent failures, got %v", err)
			}
		})
	}
}

func TestErrorClassification(t *testing.T) {
	permanent := []error{
		domain.NewValidationError("x", "y"), domain.ErrIdempotencyConflict, domain.ErrExternalTransactionConflict, errInboxConflict,
	}
	for _, e := range permanent {
		if !isPermanent(e) {
			t.Errorf("%v must be permanent", e)
		}
	}
	for _, e := range []error{domain.ErrWalletNotFound, errors.New("boom"), nil} {
		if isPermanent(e) {
			t.Errorf("%v must be retried", e)
		}
	}
	if permanentReason(domain.ErrIdempotencyConflict) != "idempotency_conflict" || permanentReason(errInboxConflict) != "message_id_conflict" ||
		permanentReason(domain.NewValidationError("a", "b")) != "invalid_message" {
		t.Error("dead-letter reasons")
	}
}

func TestNormalizeQueueURL(t *testing.T) {
	cases := []struct{ url, endpoint, want string }{
		{"http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/q.fifo", "http://localstack:4566", "http://localstack:4566/000000000000/q.fifo"},
		{"http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/q.fifo", "http://localhost:4566", "http://localhost:4566/000000000000/q.fifo"},
		{"https://sqs.us-east-1.amazonaws.com/123456789012/q.fifo", "", "https://sqs.us-east-1.amazonaws.com/123456789012/q.fifo"},
		{"http://x/y", "not a url", "http://x/y"},
	}
	for _, c := range cases {
		if got := NormalizeQueueURL(c.url, c.endpoint); got != c.want {
			t.Errorf("NormalizeQueueURL(%q, %q) = %q, want %q", c.url, c.endpoint, got, c.want)
		}
	}
}
