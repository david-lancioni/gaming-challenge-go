package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/infra/auth"
	"github.com/dlancioni/backend-challenge-go/internal/observability"
)

type TokenAuthenticator interface {
	Authenticate(ctx context.Context, authorization string) (*auth.Principal, error)
}

type HealthChecker interface {
	Name() string
	Check(ctx context.Context) error
}

type RequestMetrics interface {
	HTTPRequest(route, code string, d time.Duration)
}

type Deps struct {
	Wallets   *application.WalletService
	Wagering  *application.WageringService
	Auth      TokenAuthenticator
	Checkers  []HealthChecker
	Metrics   RequestMetrics
	Log       *slog.Logger
	Clock     application.Clock
	ReadyWait time.Duration
}

type Server struct {
	Deps
	mux *http.ServeMux
}

const maxBodyBytes = 64 << 10

type access int

const (
	accessInternal access = iota
	accessProvider
	accessAny
)

func NewServer(d Deps) *Server {
	if d.ReadyWait == 0 {
		d.ReadyWait = 2 * time.Second
	}
	if d.Clock == nil {
		d.Clock = time.Now
	}
	s := &Server{Deps: d, mux: http.NewServeMux()}

	s.mux.Handle("POST /wallets", s.secured(accessInternal, s.openWallet))
	s.mux.Handle("GET /wallets/{walletId}", s.secured(accessInternal, s.getWallet))
	s.mux.Handle("GET /wallets/{walletId}/ledger", s.secured(accessInternal, s.getLedger))
	s.mux.Handle("POST /wallets/{walletId}/reconciliation", s.secured(accessInternal, s.reconcile))

	s.mux.Handle("POST /wagering/transactions", s.secured(accessProvider, s.submitTransaction))
	s.mux.Handle("GET /wagering/transactions/{transactionId}", s.secured(accessAny, s.getTransaction))
	s.mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", s.secured(accessAny, s.getProviderTransaction))

	s.mux.HandleFunc("GET /health/live", s.live)
	s.mux.HandleFunc("GET /health/ready", s.ready)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeAPIError(w, r, http.StatusNotFound, "NOT_FOUND", "route not found", "", false)
	})
	return s
}

func (s *Server) Handler() http.Handler {
	return s.instrument(s.mux)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-Id")
		if corr == "" || len(corr) > 128 || !printable(corr) {
			corr = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", corr)
		ctx := observability.With(r.Context(), slog.String("correlationId", corr))
		r = r.WithContext(ctx)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if p := recover(); p != nil {
				s.Log.ErrorContext(ctx, "panic while serving request", slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
				if rec.status == http.StatusOK {
					writeAPIError(rec, r, http.StatusInternalServerError, "INTERNAL_ERROR", "internal error", "", false)
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			if s.Metrics != nil {
				s.Metrics.HTTPRequest(route, statusClass(rec.status), elapsed)
			}
			s.Log.InfoContext(ctx, "http request",
				slog.String("method", r.Method), slog.String("route", route),
				slog.Int("status", rec.status), slog.Float64("durationMs", float64(elapsed.Microseconds())/1000))
		}()
		next.ServeHTTP(rec, r)
	})
}

func statusClass(code int) string {
	return string(rune('0'+code/100)) + "xx"
}

func printable(s string) bool {
	for _, r := range s {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func (s *Server) secured(level access, h func(http.ResponseWriter, *http.Request, *auth.Principal)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := s.Auth.Authenticate(r.Context(), r.Header.Get("Authorization"))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wagering"`)
			writeAPIError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "missing, invalid or expired credentials", "", false)
			return
		}
		allowed := false
		switch level {
		case accessInternal:
			allowed = p.IsInternal()
		case accessProvider:
			allowed = p.IsProvider()
		case accessAny:
			allowed = p.IsInternal() || p.IsProvider()
		}
		if !allowed {
			s.Log.WarnContext(r.Context(), "forbidden", slog.String("subject", p.Subject), slog.String("route", r.Pattern))
			writeAPIError(w, r, http.StatusForbidden, "FORBIDDEN", "the caller is not allowed to perform this operation", "", false)
			return
		}
		ctx := auth.NewContext(r.Context(), p)
		if p.ProviderID != "" {
			ctx = observability.With(ctx, slog.String("providerId", p.ProviderID))
		}
		h(w, r.WithContext(ctx), p)
	})
}

func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.ReadyWait)
	defer cancel()
	checks := map[string]string{}
	status, code := "UP", http.StatusOK
	for _, c := range s.Checkers {
		if err := c.Check(ctx); err != nil {
			checks[c.Name()] = "DOWN"
			status, code = "DOWN", http.StatusServiceUnavailable
			s.Log.WarnContext(r.Context(), "readiness check failed", slog.String("check", c.Name()), slog.String("error", err.Error()))
			continue
		}
		checks[c.Name()] = "UP"
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": checks})
}
