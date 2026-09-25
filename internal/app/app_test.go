package app_test

import (
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/dlancioni/backend-challenge-go/internal/app"
	"github.com/dlancioni/backend-challenge-go/internal/config"
)

func baseConfig() config.Config {
	return config.Config{
		InstanceID: "test", LogLevel: "error", HTTPAddr: "127.0.0.1:0", MetricsAddr: "127.0.0.1:0",
		EnableHTTP: true, EnableConsumer: true, EnablePublisher: true, EnablePendingWorker: true,
		ShutdownTimeout: 5 * time.Second,
		Database:        config.DatabaseConfig{URL: "postgres://u:p@localhost/db", MaxConns: 2},
		AWS:             config.AWSConfig{Region: "us-east-1"},
		Queues: config.QueuesConfig{Input: "a.fifo", InputDLQ: "b.fifo", Events: "c.fifo",
			VisibilityTimeout: 60 * time.Second, MaxReceiveCount: 5},
		Auth: config.AuthConfig{Issuer: "http://idp/realms/x", JWKSURL: "http://idp/certs", Audience: "api",
			ProviderRole: "p", InternalRole: "i", ProviderClaim: "provider_id"},
		Consumer: config.ConsumerConfig{Name: "c", Concurrency: 1, BatchSize: 1, ProcessTimeout: time.Second},
		Outbox:   config.OutboxConfig{BatchSize: 1, PollInterval: time.Second, Lease: time.Second},
		Pending:  config.PendingConfig{TTL: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, Lease: time.Second, PollInterval: time.Second, BatchSize: 1},
	}
}

func TestFxGraphIsValidForEveryComponentCombination(t *testing.T) {
	for i := 0; i < 16; i++ {
		cfg := baseConfig()
		cfg.EnableHTTP = i&1 != 0
		cfg.EnableConsumer = i&2 != 0
		cfg.EnablePublisher = i&4 != 0
		cfg.EnablePendingWorker = i&8 != 0
		if err := fx.ValidateApp(app.Options(cfg)); err != nil {
			t.Errorf("http=%v consumer=%v publisher=%v pending=%v: %v",
				cfg.EnableHTTP, cfg.EnableConsumer, cfg.EnablePublisher, cfg.EnablePendingWorker, err)
		}
	}
}
