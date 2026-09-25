package messaging

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/config"
)

type PublisherHooks struct {
	AfterSend func(eventID string) (skipMark bool)
}

// Publisher é o relay do transactional outbox. Um evento só existe no outbox porque
// a transação de negócio que o gerou fez commit, então nada é publicado antes do
// commit. Vários publishers podem rodar ao mesmo tempo (ver outboxQueue.Claim).
//
// A entrega é at-least-once: uma queda entre SendMessage e MarkPublished republica
// o evento com o mesmo eventId, que também é o MessageDeduplicationId da fila FIFO;
// os consumidores deduplicam pelo eventId.
type Publisher struct {
	api      API
	queueURL string
	queue    application.OutboxQueue
	workerID string
	cfg      config.OutboxConfig
	log      *slog.Logger
	metrics  Metrics
	hooks    PublisherHooks

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool
}

func NewPublisher(api API, queueURL string, queue application.OutboxQueue, workerID string, cfg config.OutboxConfig,
	metrics Metrics, log *slog.Logger, hooks PublisherHooks) *Publisher {
	return &Publisher{api: api, queueURL: queueURL, queue: queue, workerID: workerID, cfg: cfg,
		metrics: metrics, log: log, hooks: hooks}
}

func (p *Publisher) Running() bool { return p.running.Load() }

func (p *Publisher) Start(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		return errors.New("publisher already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	p.running.Store(true)
	go p.run(ctx)
	p.log.Info("outbox publisher started", slog.String("workerId", p.workerID))
	return nil
}

func (p *Publisher) Stop(ctx context.Context) error {
	p.mu.Lock()
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		p.log.Info("outbox publisher stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) run(ctx context.Context) {
	defer func() { p.running.Store(false); close(p.done) }()
	var lastLag time.Time
	failures := 0
	for ctx.Err() == nil {
		if time.Since(lastLag) > 2*time.Second {
			if age, err := p.queue.OldestUnpublishedAge(ctx); err == nil {
				p.metrics.OutboxLag(age)
			}
			lastLag = time.Now()
		}
		n, err := p.tick(ctx)
		if err != nil && ctx.Err() == nil {
			failures++
			p.log.Warn("outbox tick failed", slog.String("error", err.Error()))
			p.metrics.Retry("outbox_publisher")
			p.sleep(ctx, application.Backoff(p.cfg.PollInterval, 10*time.Second, failures))
			continue
		}
		failures = 0
		if n == 0 {
			p.sleep(ctx, p.cfg.PollInterval)
		}
	}
}

func (p *Publisher) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (p *Publisher) tick(ctx context.Context) (int, error) {
	recs, err := p.queue.Claim(ctx, p.workerID, p.cfg.BatchSize, p.cfg.Lease)
	if err != nil {
		return 0, err
	}
	blocked := map[string]struct{}{}
	for i, rec := range recs {
		group := rec.PartitionKey.String()
		if ctx.Err() != nil {
			p.releaseRest(recs[i:], "released on shutdown", 0)
			return len(recs), nil
		}
		// Preserva a ordem por carteira: não publica além de um evento que falhou.
		if _, skip := blocked[group]; skip {
			p.releaseRest(recs[i:i+1], "deferred: an earlier event of the same wallet failed", time.Second)
			continue
		}
		if err := p.publish(ctx, rec); err != nil {
			blocked[group] = struct{}{}
		}
	}
	return len(recs), nil
}

func (p *Publisher) publish(ctx context.Context, rec application.OutboxRecord) error {
	log := p.log.With(slog.String("eventId", rec.ID.String()), slog.String("eventType", rec.EventType),
		slog.String("walletId", rec.PartitionKey.String()), slog.Int("attempt", rec.Attempts))
	sctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := p.api.SendMessage(sctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(rec.Payload)),
		MessageGroupId:         aws.String(rec.PartitionKey.String()),
		MessageDeduplicationId: aws.String(rec.ID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(rec.ID.String())},
		},
	})
	if err != nil {
		p.metrics.OutboxFailure()
		p.metrics.Retry("outbox_publisher")
		delay := application.Backoff(p.cfg.BackoffBase, p.cfg.BackoffMax, rec.Attempts-1)
		log.Warn("event publication failed", slog.String("error", err.Error()), slog.Duration("retryIn", delay))
		mctx, mcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer mcancel()
		if merr := p.queue.MarkFailed(mctx, rec.ID, err.Error(), delay); merr != nil {
			log.Warn("could not record publication failure", slog.String("error", merr.Error()))
		}
		return err
	}
	if p.hooks.AfterSend != nil && p.hooks.AfterSend(rec.ID.String()) {
		return nil
	}
	mctx, mcancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer mcancel()
	if err := p.queue.MarkPublished(mctx, rec.ID); err != nil {
		// O evento já está no SQS; quando o lease expirar ele será republicado com o
		// mesmo eventId, e o consumidor deduplica.
		log.Warn("event published but not confirmed in the outbox", slog.String("error", err.Error()))
		return nil
	}
	p.metrics.OutboxPublished(rec.EventType)
	log.InfoContext(ctx, "event published")
	return nil
}

func (p *Publisher) releaseRest(recs []application.OutboxRecord, cause string, retryAfter time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, rec := range recs {
		if err := p.queue.MarkFailed(ctx, rec.ID, cause, retryAfter); err != nil {
			p.log.Warn("could not release outbox event", slog.String("eventId", rec.ID.String()), slog.String("error", err.Error()))
		}
	}
}
