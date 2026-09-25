// Package app compõe o serviço com Uber Fx. É o único pacote que conhece o Fx;
// domínio, casos de uso e adaptadores não dependem dele.
//
// Ciclo de vida: os OnStart rodam na ordem de construção, que segue o grafo de
// dependências (pool e cliente SQS primeiro, entradas por último). Os OnStop rodam
// na ordem inversa: o servidor HTTP para de aceitar requisições, o consumidor para
// de buscar mensagens e conclui (ou libera) as que estão em andamento, o publisher
// e o worker de pendências param, e só então o pool de conexões é fechado.
package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/httpapi"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

func New(cfg config.Config, extra ...fx.Option) *fx.App {
	return fx.New(Options(cfg), fx.Options(extra...))
}

func Options(cfg config.Config) fx.Option {
	opts := []fx.Option{
		fx.StartTimeout(90 * time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.Supply(cfg),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger {
			fl := &fxevent.SlogLogger{Logger: l}
			fl.UseLogLevel(slog.LevelDebug)
			return fl
		}),
		observabilityModule,
		postgresModule,
		messagingModule,
		applicationModule,
	}
	opts = append(opts, workersModule(cfg))
	if cfg.EnableHTTP {
		opts = append(opts, httpModule)
	}
	return fx.Options(opts...)
}

var observabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(os.Stdout, cfg.LogLevel).With(slog.String("instanceId", cfg.InstanceID))
		},
		observability.NewMetrics,
		func(m *observability.Metrics) application.Metrics { return m },
		func(m *observability.Metrics) messaging.Metrics { return m },
		func(m *observability.Metrics) httpapi.RequestMetrics { return m },
		func() application.Clock { return time.Now },
		newMetricsServer,
	),
	fx.Invoke(func(*MetricsServer) {}),
)

var postgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		func(pool *pgxpool.Pool, m application.Metrics) application.UnitOfWork {
			return postgres.NewUnitOfWork(pool, m)
		},
		postgres.NewWalletReader,
		postgres.NewTransactionReader,
		postgres.NewPendingQueue,
		postgres.NewOutboxQueue,
		fx.Annotate(
			func(pool *pgxpool.Pool) httpapi.HealthChecker { return &postgres.Checker{Pool: pool} },
			fx.ResultTags(`group:"health"`),
		),
	),
)

var messagingModule = fx.Module("messaging",
	fx.Provide(
		func(cfg config.Config) (*sqs.Client, error) {
			return messaging.NewClient(context.Background(), cfg.AWS)
		},
		func(c *sqs.Client) messaging.API { return c },
		newQueueURLs,
		fx.Annotate(
			func(api messaging.API, urls messaging.QueueURLs) httpapi.HealthChecker {
				return &messaging.QueueChecker{API: api, URLs: []string{urls.Input, urls.Events}}
			},
			fx.ResultTags(`group:"health"`),
		),
	),
)

var applicationModule = fx.Module("application",
	fx.Provide(
		func(clock application.Clock, cfg config.Config, m application.Metrics) *application.Processor {
			return application.NewProcessor(clock, application.PendingPolicy{
				TTL: cfg.Pending.TTL, MaxAttempts: cfg.Pending.MaxAttempts,
				BaseBackoff: cfg.Pending.BackoffBase, MaxBackoff: cfg.Pending.BackoffMax,
			}, m)
		},
		application.NewWageringService,
		application.NewWalletService,
		func(uow application.UnitOfWork, q application.PendingQueue, p *application.Processor, clock application.Clock,
			cfg config.Config, m application.Metrics, log *slog.Logger) *application.PendingResolver {
			return application.NewPendingResolver(uow, q, p, clock, cfg.Pending.Lease, m, log)
		},
	),
)
