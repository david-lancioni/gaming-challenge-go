package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.uber.org/fx"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
	"github.com/dlancioni/backend-challenge-go/internal/infra/worker"
)

const (
	FaultConsumerAfterCommit = "consumer_after_commit"
	FaultPublisherAfterSend  = "publisher_after_send"
)

func share(ctx context.Context, num, den int64) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	return context.WithTimeout(ctx, remaining*time.Duration(num)/time.Duration(den))
}

// stopWithin dá a cada worker só uma fração (num/den) do prazo restante de shutdown,
// para sobrar tempo aos componentes que param depois dele, como o pool do banco.
func stopWithin(num, den int64, stop func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		sctx, cancel := share(ctx, num, den)
		defer cancel()
		return stop(sctx)
	}
}

func workersModule(cfg config.Config) fx.Option {
	var opts []fx.Option
	if cfg.EnablePendingWorker {
		opts = append(opts, fx.Provide(newPendingWorker), fx.Invoke(func(*PendingWorker) {}))
	}
	if cfg.EnablePublisher {
		opts = append(opts, fx.Provide(newPublisher), fx.Invoke(func(*messaging.Publisher) {}))
	}
	if cfg.EnableConsumer {
		opts = append(opts, fx.Provide(newConsumer), fx.Invoke(func(*messaging.Consumer) {}))
	}
	return fx.Module("workers", opts...)
}

type PendingWorker struct{ *worker.Loop }

func newPendingWorker(lc fx.Lifecycle, cfg config.Config, resolver *application.PendingResolver, m application.Metrics, log *slog.Logger) *PendingWorker {
	loop := worker.NewLoop("pending-reference", cfg.Pending.PollInterval,
		func(ctx context.Context) (int, error) { return resolver.RunOnce(ctx, cfg.Pending.BatchSize) },
		func() { m.Retry("reference_worker_round") }, log)
	lc.Append(fx.Hook{OnStart: loop.Start, OnStop: stopWithin(1, 2, loop.Stop)})
	return &PendingWorker{loop}
}

func newPublisher(lc fx.Lifecycle, api messaging.API, urls messaging.QueueURLs, q application.OutboxQueue,
	cfg config.Config, m messaging.Metrics, log *slog.Logger) *messaging.Publisher {
	var hooks messaging.PublisherHooks
	if cfg.FaultInjection == FaultPublisherAfterSend {
		log.Warn("FAULT INJECTION ENABLED: the process will exit after the first event is sent", slog.String("mode", cfg.FaultInjection))
		hooks.AfterSend = func(string) bool { os.Exit(137); return true }
	}
	p := messaging.NewPublisher(api, urls.Events, q, cfg.InstanceID, cfg.Outbox, m, log, hooks)
	lc.Append(fx.Hook{OnStart: p.Start, OnStop: stopWithin(1, 2, p.Stop)})
	return p
}

func newConsumer(lc fx.Lifecycle, api messaging.API, urls messaging.QueueURLs, uow application.UnitOfWork,
	proc *application.Processor, clock application.Clock, cfg config.Config, m messaging.Metrics, log *slog.Logger) *messaging.Consumer {
	var hooks messaging.ConsumerHooks
	if cfg.FaultInjection == FaultConsumerAfterCommit {
		log.Warn("FAULT INJECTION ENABLED: the process will exit after the first message commits", slog.String("mode", cfg.FaultInjection))
		hooks.AfterCommit = func(string) bool { os.Exit(137); return true }
	}
	c := messaging.NewConsumer(api, urls, uow, proc, clock, cfg.Consumer, cfg.Queues, m, log, hooks)
	lc.Append(fx.Hook{OnStart: c.Start, OnStop: stopWithin(1, 2, c.Stop)})
	return c
}
