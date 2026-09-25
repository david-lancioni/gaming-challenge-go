package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/config"
	"github.com/dlancioni/backend-challenge-go/internal/infra/auth"
	"github.com/dlancioni/backend-challenge-go/internal/infra/httpapi"
)

var httpModule = fx.Module("http",
	fx.Provide(
		func(cfg config.Config) *auth.Authenticator { return auth.New(context.Background(), cfg.Auth) },
		func(a *auth.Authenticator) httpapi.TokenAuthenticator { return a },
		newAPIServer,
	),
	fx.Invoke(func(*APIServer) {}),
)

type APIServer struct {
	srv  *http.Server
	addr string
}

func (s *APIServer) Addr() string { return s.addr }

type apiParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Cfg       config.Config
	Wallets   *application.WalletService
	Wagering  *application.WageringService
	Auth      httpapi.TokenAuthenticator
	Checkers  []httpapi.HealthChecker `group:"health"`
	Metrics   httpapi.RequestMetrics
	Log       *slog.Logger
	Clock     application.Clock
}

func newAPIServer(p apiParams) *APIServer {
	handler := httpapi.NewServer(httpapi.Deps{
		Wallets: p.Wallets, Wagering: p.Wagering, Auth: p.Auth, Checkers: p.Checkers,
		Metrics: p.Metrics, Log: p.Log, Clock: p.Clock,
	}).Handler()
	s := &APIServer{srv: &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ln, err := net.Listen("tcp", p.Cfg.HTTPAddr)
			if err != nil {
				return fmt.Errorf("http listen: %w", err)
			}
			s.addr = ln.Addr().String()
			go func() {
				if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					p.Log.Error("http server failed", slog.String("error", err.Error()))
				}
			}()
			p.Log.Info("http server listening", slog.String("addr", s.addr))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			ctx, cancel := share(ctx, 1, 2)
			defer cancel()
			err := s.srv.Shutdown(ctx)
			if err != nil {
				_ = s.srv.Close()
			}
			p.Log.Info("http server stopped")
			return err
		},
	})
	return s
}
