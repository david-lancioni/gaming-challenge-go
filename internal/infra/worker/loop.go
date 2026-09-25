package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dlancioni/backend-challenge-go/internal/application"
)

type Loop struct {
	name     string
	interval time.Duration
	work     func(ctx context.Context) (int, error)
	log      *slog.Logger
	onRetry  func()

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool
}

func NewLoop(name string, interval time.Duration, work func(ctx context.Context) (int, error), onRetry func(), log *slog.Logger) *Loop {
	return &Loop{name: name, interval: interval, work: work, log: log, onRetry: onRetry}
}

func (l *Loop) Running() bool { return l.running.Load() }

func (l *Loop) Start(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		return errors.New(l.name + " already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel, l.done = cancel, make(chan struct{})
	l.running.Store(true)
	go l.run(ctx)
	l.log.Info("worker started", slog.String("worker", l.name))
	return nil
}

func (l *Loop) Stop(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		l.log.Info("worker stopped", slog.String("worker", l.name))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *Loop) run(ctx context.Context) {
	defer func() { l.running.Store(false); close(l.done) }()
	failures := 0
	for ctx.Err() == nil {
		n, err := l.work(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			failures++
			if l.onRetry != nil {
				l.onRetry()
			}
			l.log.Warn("worker round failed", slog.String("worker", l.name), slog.String("error", err.Error()))
			l.sleep(ctx, application.Backoff(l.interval, 15*time.Second, failures))
		case n == 0:
			failures = 0
			l.sleep(ctx, l.interval)
		default:
			failures = 0
		}
	}
}

func (l *Loop) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
