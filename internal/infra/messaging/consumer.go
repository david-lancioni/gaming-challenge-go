package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

const MessageType = "WagerTransactionRequested"

type Metrics interface {
	application.Metrics
	DeadLetter(reason string)
	SQSMessage(outcome string)
	OutboxLag(d time.Duration)
	OutboxPublished(eventType string)
	OutboxFailure()
}

type ConsumerHooks struct {
	AfterCommit func(messageID string) (skipAck bool)
}

type envelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

type requestData struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId"`
}

type parsedMessage struct {
	MessageID string
	Params    domain.ExternalParams
	InboxHash string
}

var errInboxConflict = errors.New("message id reused with different content")

func ParseMessage(body string, now time.Time) (parsedMessage, error) {
	var env envelope
	if err := strictDecode([]byte(body), &env); err != nil {
		return parsedMessage{}, domain.NewValidationError("envelope", err.Error())
	}
	if err := domain.ValidateToken("messageId", env.MessageID, 128); err != nil {
		return parsedMessage{}, err
	}
	if env.Type != MessageType {
		return parsedMessage{}, domain.NewValidationError("type", fmt.Sprintf("must be %q", MessageType))
	}
	if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
		return parsedMessage{}, domain.NewValidationError("occurredAt", "must be an RFC 3339 timestamp")
	}
	var d requestData
	if err := strictDecode(env.Data, &d); err != nil {
		if domain.IsMoneyError(err) {
			return parsedMessage{}, domain.NewValidationErrorWrap("data.money", err)
		}
		return parsedMessage{}, domain.NewValidationError("data", err.Error())
	}
	params := domain.ExternalParams{
		ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey,
		PlayerID: d.PlayerID, WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID,
		Kind: domain.TransactionKind(d.Kind), Money: d.Money,
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
		CorrelationID:                  env.MessageID, CausationID: env.MessageID,
	}
	// Mesma validação e mesmo hash canônico do HTTP: uma operação enviada pelos dois
	// canais é reconhecida como a mesma.
	probe, err := domain.NewExternalTransaction(params, now)
	if err != nil {
		return parsedMessage{}, err
	}
	// Hash da inbox, usado para detectar reentrega do mesmo messageId com conteúdo
	// diferente: sha256(type | idempotencyKey | payloadHash).
	sum := sha256.Sum256([]byte(env.Type + "\n" + d.IdempotencyKey + "\n" + probe.PayloadHash()))
	return parsedMessage{MessageID: env.MessageID, Params: params, InboxHash: hex.EncodeToString(sum[:])}, nil
}

func strictDecode(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("empty JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after the JSON object")
	}
	return nil
}

// Consumer lê operações da fila FIFO de entrada, com entrega at-least-once.
//
// A mensagem só é removida da fila depois do commit do seu tratamento durável:
// inbox, alterações de domínio, ledger e eventos do outbox compartilham uma única
// transação SQL. Rejeições de negócio são resultados confirmados e também removem
// a mensagem. Falhas transitórias voltam com backoff (ChangeMessageVisibility);
// falhas permanentes e tentativas esgotadas vão para a DLQ. A redrive policy da
// fila é a rede de segurança se o próprio envio à DLQ falhar.
type Consumer struct {
	api     API
	urls    QueueURLs
	uow     application.UnitOfWork
	proc    *application.Processor
	clock   application.Clock
	cfg     config.ConsumerConfig
	queues  config.QueuesConfig
	log     *slog.Logger
	metrics Metrics
	hooks   ConsumerHooks

	mu         sync.Mutex
	pollCancel context.CancelFunc
	workCancel context.CancelFunc
	pollCtx    context.Context
	workCtx    context.Context
	wg         sync.WaitGroup
	running    atomic.Bool
}

func NewConsumer(api API, urls QueueURLs, uow application.UnitOfWork, proc *application.Processor, clock application.Clock,
	cfg config.ConsumerConfig, queues config.QueuesConfig, metrics Metrics, log *slog.Logger, hooks ConsumerHooks) *Consumer {
	return &Consumer{api: api, urls: urls, uow: uow, proc: proc, clock: clock, cfg: cfg, queues: queues,
		metrics: metrics, log: log, hooks: hooks}
}

func (c *Consumer) Running() bool { return c.running.Load() }

func (c *Consumer) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pollCancel != nil {
		return errors.New("consumer already started")
	}
	c.pollCtx, c.pollCancel = context.WithCancel(context.Background())
	c.workCtx, c.workCancel = context.WithCancel(context.Background())
	c.running.Store(true)
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.wg.Add(1)
		go c.poll(i)
	}
	c.log.Info("sqs consumer started", slog.Int("concurrency", c.cfg.Concurrency), slog.String("queue", c.urls.Input))
	return nil
}

// Stop para de buscar mensagens e espera as que estão em processamento até o prazo
// de ctx. Se o prazo vencer, o trabalho em andamento é cancelado (a transação SQL
// sofre rollback) e as mensagens são liberadas para reentrega segura.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	pollCancel, workCancel := c.pollCancel, c.workCancel
	c.mu.Unlock()
	if pollCancel == nil {
		return nil
	}
	pollCancel()
	done := make(chan struct{})
	go func() { c.wg.Wait(); c.running.Store(false); close(done) }()
	select {
	case <-done:
		workCancel()
		c.log.Info("sqs consumer stopped")
		return nil
	case <-ctx.Done():
		workCancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		c.log.Warn("sqs consumer stopped after the shutdown deadline; in-flight messages were released")
		return ctx.Err()
	}
}

func (c *Consumer) poll(worker int) {
	defer c.wg.Done()
	for c.pollCtx.Err() == nil {
		out, err := c.api.ReceiveMessage(c.pollCtx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.urls.Input),
			MaxNumberOfMessages:         int32(c.cfg.BatchSize),
			WaitTimeSeconds:             int32(c.cfg.WaitTime.Seconds()),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameMessageGroupId},
		})
		if err != nil {
			if c.pollCtx.Err() != nil {
				return
			}
			c.log.Warn("receive failed", slog.Int("worker", worker), slog.String("error", err.Error()))
			c.metrics.Retry("sqs_receive")
			select {
			case <-time.After(application.Backoff(500*time.Millisecond, 10*time.Second, 2)):
			case <-c.pollCtx.Done():
			}
			continue
		}
		for i, m := range out.Messages {
			if c.pollCtx.Err() != nil {
				c.release(out.Messages[i:]...)
				break
			}
			// Após uma falha transitória, o resto do lote é devolvido sem processar
			// para preservar a ordem por MessageGroupId (carteira).
			if stopBatch := c.handle(m); stopBatch {
				c.release(out.Messages[i+1:]...)
				break
			}
		}
	}
}

func (c *Consumer) release(msgs ...types.Message) {
	for _, m := range msgs {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.pollCtx), 5*time.Second)
		if _, err := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(c.urls.Input), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0}); err != nil {
			c.log.Warn("could not release message", slog.String("error", err.Error()))
		} else {
			c.metrics.SQSMessage("released")
		}
		cancel()
	}
}

func receiveCount(m types.Message) int {
	n, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if n < 1 {
		n = 1
	}
	return n
}

func (c *Consumer) handle(m types.Message) (stopBatch bool) {
	body := aws.ToString(m.Body)
	msg, err := ParseMessage(body, c.clock())
	if err != nil {
		c.log.Warn("invalid message", slog.String("sqsMessageId", aws.ToString(m.MessageId)), slog.String("reason", err.Error()))
		c.deadLetter(m, "invalid_message", err)
		return false
	}

	ctx := observability.With(c.workCtx,
		slog.String("messageId", msg.MessageID), slog.String("correlationId", msg.MessageID),
		slog.String("providerId", msg.Params.ProviderID), slog.String("walletId", msg.Params.WalletID))
	ctx, cancel := context.WithTimeout(ctx, c.cfg.ProcessTimeout)
	defer cancel()

	start := time.Now()
	var out application.Outcome
	var duplicate bool
	err = c.uow.Do(ctx, func(ctx context.Context, r application.Repositories) error {
		duplicate = false
		inserted, existingHash, err := r.Inbox().Begin(ctx, c.cfg.Name, msg.MessageID, msg.InboxHash, c.clock())
		if err != nil {
			return err
		}
		if !inserted {
			if existingHash != msg.InboxHash {
				return errInboxConflict
			}
			// Já tratada e confirmada antes: só confirma a remoção da fila.
			duplicate = true
			return nil
		}
		o, err := c.proc.ProcessExternal(ctx, r, msg.Params)
		if err != nil {
			return err
		}
		out = o
		return r.Inbox().Complete(ctx, c.cfg.Name, msg.MessageID, c.clock())
	})

	switch {
	case err == nil && duplicate:
		c.metrics.Duplicate(application.SourceSQS, "replay")
		c.metrics.SQSMessage("duplicate")
		c.log.InfoContext(ctx, "duplicate message acknowledged")
	case err == nil:
		c.proc.RecordOutcome(application.SourceSQS, out, time.Since(start))
		c.metrics.SQSMessage("processed")
		t := out.Transaction
		c.log.InfoContext(observability.With(ctx, slog.String("transactionId", t.ID().String())), "message processed",
			slog.String("status", string(t.Status())), slog.String("kind", string(t.Kind())),
			slog.String("failureCode", string(t.FailureCode())), slog.Bool("replay", out.Replay))
	case isPermanent(err):
		if errors.Is(err, domain.ErrIdempotencyConflict) || errors.Is(err, domain.ErrExternalTransactionConflict) {
			c.metrics.Duplicate(application.SourceSQS, "conflict")
		}
		c.log.WarnContext(ctx, "permanent failure", slog.String("error", err.Error()))
		c.deadLetter(m, permanentReason(err), err)
		return false
	default:
		return c.retry(ctx, m, err)
	}

	// O tratamento durável já foi confirmado. Uma queda neste ponto só causa
	// reentrega, que a inbox transforma em confirmação sem reprocessar.
	if c.hooks.AfterCommit != nil && c.hooks.AfterCommit(msg.MessageID) {
		return false
	}
	c.ack(ctx, m)
	return false
}

func isPermanent(err error) bool {
	return errors.Is(err, domain.ErrInvalidInput) ||
		errors.Is(err, domain.ErrIdempotencyConflict) ||
		errors.Is(err, domain.ErrExternalTransactionConflict) ||
		errors.Is(err, errInboxConflict)
}

func permanentReason(err error) string {
	switch {
	case errors.Is(err, domain.ErrIdempotencyConflict), errors.Is(err, domain.ErrExternalTransactionConflict):
		return "idempotency_conflict"
	case errors.Is(err, errInboxConflict):
		return "message_id_conflict"
	}
	return "invalid_message"
}

func (c *Consumer) retry(ctx context.Context, m types.Message, cause error) (stopBatch bool) {
	count := receiveCount(m)
	shuttingDown := c.pollCtx.Err() != nil || errors.Is(cause, context.Canceled)
	c.log.WarnContext(ctx, "transient failure while processing message",
		slog.String("error", cause.Error()), slog.Int("receiveCount", count))
	if shuttingDown {
		c.release(m)
		return true
	}
	if count >= c.queues.MaxReceiveCount {
		c.deadLetter(m, "retries_exhausted", cause)
		return true
	}
	c.metrics.Retry("sqs_consumer")
	c.metrics.SQSMessage("retry")
	delay := application.Backoff(c.cfg.RetryBase, c.cfg.RetryMax, count-1)
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := c.api.ChangeMessageVisibility(cctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.urls.Input), ReceiptHandle: m.ReceiptHandle,
		VisibilityTimeout: int32(delay.Seconds()) + 1}); err != nil {
		c.log.WarnContext(ctx, "could not set retry delay", slog.String("error", err.Error()))
	}
	return true
}

func (c *Consumer) deadLetter(m types.Message, reason string, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.pollCtx), 10*time.Second)
	defer cancel()
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	errText := cause.Error()
	if len(errText) > 500 {
		errText = errText[:500]
	}
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.urls.InputDLQ),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String("dlq-" + aws.ToString(m.MessageId)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"dlqReason":         {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"dlqError":          {DataType: aws.String("String"), StringValue: aws.String(errText)},
			"originalMessageId": {DataType: aws.String("String"), StringValue: m.MessageId},
		},
	})
	if err != nil {
		c.log.Error("could not dead-letter message; leaving it to the redrive policy", slog.String("error", err.Error()))
		return
	}
	c.metrics.DeadLetter(reason)
	c.metrics.SQSMessage("dead_lettered")
	c.ack(ctx, m)
}

func (c *Consumer) ack(ctx context.Context, m types.Message) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := c.api.DeleteMessage(dctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.urls.Input), ReceiptHandle: m.ReceiptHandle}); err != nil {
		c.log.WarnContext(ctx, "could not delete message; it will be redelivered", slog.String("error", err.Error()))
	}
}
