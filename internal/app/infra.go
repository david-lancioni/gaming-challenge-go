package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"

	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
	"github.com/dlancioni/backend-challenge-go/internal/infra/postgres"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), postgres.PoolConfig{
		URL: cfg.Database.URL, MaxConns: cfg.Database.MaxConns,
		LockTimeout: cfg.Database.LockTimeout, StatementTimeout: cfg.Database.StatementTimeout,
		IdleInTxTimeout: cfg.Database.IdleInTxTimeout, ApplicationName: "wagering-" + cfg.InstanceID,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := waitFor(ctx, "postgres", log, func(ctx context.Context) error { return postgres.VerifySchema(ctx, pool) }); err != nil {
				return err
			}
			log.Info("postgres ready")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { pool.Close(); close(done) }()
			select {
			case <-done:
				log.Info("postgres pool closed")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	return pool, nil
}

func waitFor(ctx context.Context, name string, log *slog.Logger, check func(context.Context) error) error {
	var last error
	for attempt := 0; ; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		last = check(cctx)
		cancel()
		if last == nil {
			return nil
		}
		log.Warn("dependency not ready", slog.String("dependency", name), slog.String("error", last.Error()))
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return fmt.Errorf("%s is not available: %w", name, last)
		}
	}
}

func newQueueURLs(api messaging.API, cfg config.Config, log *slog.Logger) (messaging.QueueURLs, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var urls messaging.QueueURLs
	err := waitFor(ctx, "sqs queues", log, func(ctx context.Context) (err error) {
		urls, err = messaging.ResolveQueues(ctx, api, cfg.AWS.Endpoint, cfg.Queues, true, true)
		return err
	})
	if err != nil {
		return messaging.QueueURLs{}, err
	}
	return urls, nil
}

type MetricsServer struct {
	srv  *http.Server
	addr string
}

func (m *MetricsServer) Addr() string { return m.addr }

func newMetricsServer(lc fx.Lifecycle, cfg config.Config, m *observability.Metrics, log *slog.Logger) *MetricsServer {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	ms := &MetricsServer{srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", cfg.MetricsAddr)
			if err != nil {
				return fmt.Errorf("metrics listen: %w", err)
			}
			ms.addr = ln.Addr().String()
			go func() {
				if err := ms.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("metrics server failed", slog.String("error", err.Error()))
				}
			}()
			log.Info("metrics server listening", slog.String("addr", ms.addr))
			return nil
		},
		OnStop: func(ctx context.Context) error { return ms.srv.Shutdown(ctx) },
	})
	return ms
}
