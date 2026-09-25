//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/dlancioni/backend-challenge-go/internal/app"
	"github.com/dlancioni/backend-challenge-go/internal/infra/messaging"
)

type inProcess struct {
	app       *fx.App
	pool      *pgxpool.Pool
	consumer  *messaging.Consumer
	publisher *messaging.Publisher
	pending   *app.PendingWorker
	api       *app.APIServer
	metrics   *app.MetricsServer
	base      string
}

type parts struct {
	fx.In
	Pool      *pgxpool.Pool
	Consumer  *messaging.Consumer  `optional:"true"`
	Publisher *messaging.Publisher `optional:"true"`
	Pending   *app.PendingWorker   `optional:"true"`
	API       *app.APIServer       `optional:"true"`
	Metrics   *app.MetricsServer
}

func (s *stack) startInProcess(t *testing.T, overrides map[string]string) *inProcess {
	t.Helper()
	env := s.envFor(map[string]string{"LOG_LEVEL": "info"})
	for k, v := range overrides {
		env[k] = v
	}
	cfg := loadConfig(t, env)
	p := &inProcess{}
	var got parts
	p.app = app.New(cfg, fx.Populate(&got))
	if err := p.app.Err(); err != nil {
		t.Fatalf("fx graph: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := p.app.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	p.pool, p.consumer, p.publisher, p.pending, p.api, p.metrics = got.Pool, got.Consumer, got.Publisher, got.Pending, got.API, got.Metrics
	p.base = "http://" + p.api.Addr()
	t.Cleanup(func() {
		sctx, scancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer scancel()
		_ = p.app.Stop(sctx)
	})
	return p
}

func TestFxCompositionStartsAndReleasesEveryResource(t *testing.T) {
	s := newStack(t)
	p := s.startInProcess(t, nil)

	ready := call(t, http.MethodGet, p.base+"/health/ready", "", nil, nil)
	wantStatus(t, ready, http.StatusOK)
	if !p.consumer.Running() || !p.publisher.Running() || !p.pending.Running() {
		t.Fatalf("workers running: consumer=%v publisher=%v pending=%v", p.consumer.Running(), p.publisher.Running(), p.pending.Running())
	}
	w := openWallet(t, p.base, "10.00")
	wantStatus(t, submit(t, p.base, newOp(w, "BET", "1.00")), http.StatusOK)
	if err := p.pool.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	metricsURL := "http://" + p.metrics.Addr() + "/metrics"
	wantStatus(t, call(t, http.MethodGet, metricsURL, "", nil, nil), http.StatusOK)

	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := p.app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if p.consumer.Running() || p.publisher.Running() || p.pending.Running() {
		t.Fatalf("workers still running after stop: consumer=%v publisher=%v pending=%v", p.consumer.Running(), p.publisher.Running(), p.pending.Running())
	}
	if err := p.pool.Ping(context.Background()); err == nil {
		t.Fatal("the connection pool must be closed after stop")
	}
	for name, addr := range map[string]string{"http": p.api.Addr(), "metrics": p.metrics.Addr()} {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.Close()
			t.Fatalf("%s listener is still open after stop", name)
		}
	}
	if err := p.app.Stop(stopCtx); err != nil {
		t.Logf("second stop: %v", err)
	}
}

func TestFxFailsFastOnInvalidDependencies(t *testing.T) {
	s := newStack(t)
	cfg := loadConfig(t, s.envFor(map[string]string{"LOG_LEVEL": "warn"}))
	cfg.Database.URL = "postgres://nobody:wrong@127.0.0.1:1/none?sslmode=disable&connect_timeout=1"
	a := app.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Start(ctx); err == nil {
		_ = a.Stop(context.Background())
		t.Fatal("start must fail when the database is unreachable")
	}

	unmigrated := loadConfig(t, s.envFor(map[string]string{"LOG_LEVEL": "warn"}))
	if err := s.exec(`DROP TABLE outbox_events`); err != nil {
		t.Fatal(err)
	}
	b := app.New(unmigrated)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	if err := b.Start(ctx2); err == nil {
		_ = b.Stop(context.Background())
		t.Fatal("start must fail when the schema is not migrated")
	}
}

func (s *stack) waitForLockWaiters(t *testing.T, n int64) {
	t.Helper()
	eventually(t, 20*time.Second, "operations to be blocked on the wallet lock", func() bool {
		return s.queryInt(`SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
			AND wait_event_type = 'Lock' AND query ILIKE '%FROM wallets%FOR UPDATE%'`) >= n
	})
}

func TestGracefulShutdownCompletesInFlightWork(t *testing.T) {
	s := newStack(t)
	p := s.startInProcess(t, nil)
	w := openWallet(t, p.base, "100.00")

	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE`, w.id); err != nil {
		t.Fatal(err)
	}
	httpOp, sqsOp := newOp(w, "BET", "10.00"), newOp(w, "BET", "20.00")
	httpDone := make(chan response, 1)
	go func() { httpDone <- submit(t, p.base, httpOp) }()
	s.sendRequest(t, "inflight-1", sqsOp)
	s.waitForLockWaiters(t, 2)

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		stopped <- p.app.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		t.Fatalf("shutdown finished while work was in flight: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	if c, err := net.DialTimeout("tcp", p.api.Addr(), time.Second); err == nil {
		c.Close()
		t.Error("the listener must stop accepting new connections during shutdown")
	}

	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("shutdown did not complete after the in-flight work was released")
	}
	r := <-httpDone
	wantStatus(t, r, http.StatusOK)
	if s.txStatus(sqsOp.ext) != "PROCESSED" || s.txStatus(httpOp.ext) != "PROCESSED" {
		t.Fatalf("in-flight work was lost: sqs=%q http=%q", s.txStatus(sqsOp.ext), s.txStatus(httpOp.ext))
	}
	if s.balanceMinor(w.id) != 7000 || s.debits(w.id) != 2 {
		t.Fatalf("balance=%d debits=%d", s.balanceMinor(w.id), s.debits(w.id))
	}
	eventually(t, 10*time.Second, "the message to be acknowledged", func() bool { return s.queueDepth(t, s.urls.Input) == 0 })
	s.assertConsistent(w.id)
}

func TestShutdownDeadlineReleasesInFlightMessages(t *testing.T) {
	s := newStack(t)
	p := s.startInProcess(t, map[string]string{"ENABLE_PUBLISHER": "false", "ENABLE_PENDING_WORKER": "false"})
	w := openWallet(t, p.base, "100.00")

	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE`, w.id); err != nil {
		t.Fatal(err)
	}
	bet := newOp(w, "BET", "25.00")
	s.sendRequest(t, "deadline-1", bet)
	s.waitForLockWaiters(t, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := time.Now()
	err = p.app.Stop(ctx)
	if err == nil {
		t.Error("a shutdown that had to abandon in-flight work must report it")
	}
	if time.Since(start) > 12*time.Second {
		t.Fatalf("shutdown ignored its deadline (%s)", time.Since(start))
	}
	eventually(t, 10*time.Second, "the consumer to be stopped", func() bool { return !p.consumer.Running() })
	eventually(t, 10*time.Second, "the pool to be closed", func() bool { return p.pool.Ping(context.Background()) != nil })
	if s.txStatus(bet.ext) != "" {
		t.Fatal("canceled work must not be committed")
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	s.startProcess(t, "successor", map[string]string{"ENABLE_PUBLISHER": "false"})
	s.awaitStatus(t, bet.ext, "PROCESSED")
	eventually(t, 20*time.Second, "the message to be acknowledged", func() bool { return s.queueDepth(t, s.urls.Input) == 0 })
	if s.debits(w.id) != 1 || s.balanceMinor(w.id) != 7500 {
		t.Fatalf("debits=%d balance=%d", s.debits(w.id), s.balanceMinor(w.id))
	}
	s.assertConsistent(w.id)
}
