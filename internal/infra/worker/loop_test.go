package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func newLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestLoopRunsUntilStoppedAndReportsTermination(t *testing.T) {
	var runs atomic.Int32
	l := NewLoop("test", 5*time.Millisecond, func(ctx context.Context) (int, error) {
		runs.Add(1)
		return 0, nil
	}, nil, newLog())
	if l.Running() {
		t.Fatal("not running before Start")
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err == nil {
		t.Fatal("a loop cannot be started twice")
	}
	deadline := time.Now().Add(2 * time.Second)
	for runs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if runs.Load() < 3 || !l.Running() {
		t.Fatalf("runs=%d running=%v", runs.Load(), l.Running())
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := l.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if l.Running() {
		t.Fatal("the worker must be observably terminated after Stop")
	}
	if err := l.Stop(stopCtx); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestLoopCancelsWorkInProgressAndRetriesFailures(t *testing.T) {
	var failures atomic.Int32
	retried := make(chan struct{}, 8)
	l := NewLoop("flaky", time.Millisecond, func(ctx context.Context) (int, error) {
		if failures.Add(1) <= 2 {
			return 0, errors.New("boom")
		}
		<-ctx.Done()
		return 0, ctx.Err()
	}, func() { retried <- struct{}{} }, newLog())
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-retried:
		case <-time.After(3 * time.Second):
			t.Fatal("failed rounds must be reported and retried")
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Stop(stopCtx); err != nil {
		t.Fatalf("stop must interrupt the round in progress: %v", err)
	}
}
