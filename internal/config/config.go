package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	InstanceID string
	LogLevel   string

	HTTPAddr    string
	MetricsAddr string

	EnableHTTP          bool
	EnableConsumer      bool
	EnablePublisher     bool
	EnablePendingWorker bool

	ShutdownTimeout time.Duration

	Database DatabaseConfig
	AWS      AWSConfig
	Queues   QueuesConfig
	Auth     AuthConfig
	Consumer ConsumerConfig
	Outbox   OutboxConfig
	Pending  PendingConfig

	FaultInjection string
}

type DatabaseConfig struct {
	URL              string
	MaxConns         int32
	LockTimeout      time.Duration
	StatementTimeout time.Duration
	IdleInTxTimeout  time.Duration
}

type AWSConfig struct {
	Region   string
	Endpoint string
}

type QueuesConfig struct {
	Input                  string
	InputDLQ               string
	Events                 string
	VisibilityTimeout      time.Duration
	MaxReceiveCount        int
	ProducerPrincipal      string
	ServicePrincipal       string
	EventConsumerPrincipal string
}

type AuthConfig struct {
	Issuer        string
	JWKSURL       string
	Audience      string
	ProviderRole  string
	InternalRole  string
	ProviderClaim string
}

type ConsumerConfig struct {
	Name           string
	Concurrency    int
	BatchSize      int
	WaitTime       time.Duration
	ProcessTimeout time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
}

type OutboxConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	BackoffBase  time.Duration
	BackoffMax   time.Duration
}

type PendingConfig struct {
	TTL          time.Duration
	MaxAttempts  int
	BackoffBase  time.Duration
	BackoffMax   time.Duration
	PollInterval time.Duration
	Lease        time.Duration
	BatchSize    int
}

type loader struct {
	errs []error
}

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (l *loader) boolean(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}

func (l *loader) integer(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func Load() (Config, error) {
	c, err := loadRaw()
	if err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func LoadUnvalidated() (Config, error) { return loadRaw() }

func loadRaw() (Config, error) {
	l := &loader{}
	host, _ := os.Hostname()
	c := Config{
		InstanceID:          l.str("INSTANCE_ID", fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])),
		LogLevel:            l.str("LOG_LEVEL", "info"),
		HTTPAddr:            l.str("HTTP_ADDR", ":8080"),
		MetricsAddr:         l.str("METRICS_ADDR", ":9100"),
		EnableHTTP:          l.boolean("ENABLE_HTTP", true),
		EnableConsumer:      l.boolean("ENABLE_CONSUMER", true),
		EnablePublisher:     l.boolean("ENABLE_PUBLISHER", true),
		EnablePendingWorker: l.boolean("ENABLE_PENDING_WORKER", true),
		ShutdownTimeout:     l.duration("SHUTDOWN_TIMEOUT", 25*time.Second),
		FaultInjection:      l.str("FAULT_INJECTION", ""),
		Database: DatabaseConfig{
			URL:              l.str("DATABASE_URL", ""),
			MaxConns:         int32(l.integer("DB_MAX_CONNS", 20)),
			LockTimeout:      l.duration("DB_LOCK_TIMEOUT", 5*time.Second),
			StatementTimeout: l.duration("DB_STATEMENT_TIMEOUT", 15*time.Second),
			IdleInTxTimeout:  l.duration("DB_IDLE_IN_TX_TIMEOUT", 30*time.Second),
		},
		AWS: AWSConfig{
			Region:   l.str("AWS_REGION", "us-east-1"),
			Endpoint: l.str("AWS_ENDPOINT_URL", ""),
		},
		Queues: QueuesConfig{
			Input:                  l.str("SQS_INPUT_QUEUE", "wager-transactions.fifo"),
			InputDLQ:               l.str("SQS_INPUT_DLQ", "wager-transactions-dlq.fifo"),
			Events:                 l.str("SQS_EVENTS_QUEUE", "wager-events.fifo"),
			VisibilityTimeout:      l.duration("SQS_VISIBILITY_TIMEOUT", 60*time.Second),
			MaxReceiveCount:        l.integer("SQS_MAX_RECEIVE_COUNT", 5),
			ProducerPrincipal:      l.str("SQS_PRODUCER_PRINCIPAL", "arn:aws:iam::000000000000:user/wager-producer"),
			ServicePrincipal:       l.str("SQS_SERVICE_PRINCIPAL", "arn:aws:iam::000000000000:user/wagering-service"),
			EventConsumerPrincipal: l.str("SQS_EVENT_CONSUMER_PRINCIPAL", "arn:aws:iam::000000000000:user/event-consumer"),
		},
		Auth: AuthConfig{
			Issuer:        l.str("OIDC_ISSUER", ""),
			JWKSURL:       l.str("OIDC_JWKS_URL", ""),
			Audience:      l.str("OIDC_AUDIENCE", "wagering-api"),
			ProviderRole:  l.str("OIDC_ROLE_PROVIDER", "wagering-provider"),
			InternalRole:  l.str("OIDC_ROLE_INTERNAL", "wagering-internal"),
			ProviderClaim: l.str("OIDC_PROVIDER_CLAIM", "provider_id"),
		},
		Consumer: ConsumerConfig{
			Name:           l.str("CONSUMER_NAME", "wager-transactions"),
			Concurrency:    l.integer("CONSUMER_CONCURRENCY", 4),
			BatchSize:      l.integer("CONSUMER_BATCH_SIZE", 5),
			WaitTime:       l.duration("CONSUMER_WAIT_TIME", 5*time.Second),
			ProcessTimeout: l.duration("CONSUMER_PROCESS_TIMEOUT", 30*time.Second),
			RetryBase:      l.duration("CONSUMER_RETRY_BASE", 2*time.Second),
			RetryMax:       l.duration("CONSUMER_RETRY_MAX", 60*time.Second),
		},
		Outbox: OutboxConfig{
			BatchSize:    l.integer("OUTBOX_BATCH_SIZE", 20),
			PollInterval: l.duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
			Lease:        l.duration("OUTBOX_LEASE", 30*time.Second),
			BackoffBase:  l.duration("OUTBOX_BACKOFF_BASE", time.Second),
			BackoffMax:   l.duration("OUTBOX_BACKOFF_MAX", 5*time.Minute),
		},
		Pending: PendingConfig{
			TTL:          l.duration("PENDING_TTL", 10*time.Minute),
			MaxAttempts:  l.integer("PENDING_MAX_ATTEMPTS", 10),
			BackoffBase:  l.duration("PENDING_BACKOFF_BASE", time.Second),
			BackoffMax:   l.duration("PENDING_BACKOFF_MAX", time.Minute),
			PollInterval: l.duration("PENDING_POLL_INTERVAL", time.Second),
			Lease:        l.duration("PENDING_LEASE", 30*time.Second),
			BatchSize:    l.integer("PENDING_BATCH_SIZE", 20),
		},
	}
	if len(l.errs) > 0 {
		return Config{}, errors.Join(l.errs...)
	}
	return c, nil
}

func (c Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.Database.URL == "" {
		add("DATABASE_URL is required")
	}
	if c.Database.MaxConns < 1 {
		add("DB_MAX_CONNS must be >= 1")
	}
	if c.EnableHTTP {
		for name, v := range map[string]string{"OIDC_ISSUER": c.Auth.Issuer, "OIDC_JWKS_URL": c.Auth.JWKSURL} {
			if v == "" {
				add("%s is required when ENABLE_HTTP=true (authentication is mandatory)", name)
			} else if u, err := url.Parse(v); err != nil || u.Scheme == "" || u.Host == "" {
				add("%s must be an absolute URL", name)
			}
		}
		if c.Auth.Audience == "" || c.Auth.ProviderRole == "" || c.Auth.InternalRole == "" || c.Auth.ProviderClaim == "" {
			add("OIDC audience, roles and provider claim must not be empty")
		}
		if c.HTTPAddr == "" {
			add("HTTP_ADDR must not be empty")
		}
	}
	if c.Consumer.Concurrency < 1 || c.Consumer.BatchSize < 1 || c.Consumer.BatchSize > 10 {
		add("CONSUMER_CONCURRENCY must be >= 1 and CONSUMER_BATCH_SIZE within 1..10")
	}
	if c.Consumer.ProcessTimeout >= c.Queues.VisibilityTimeout {
		add("CONSUMER_PROCESS_TIMEOUT (%s) must be lower than SQS_VISIBILITY_TIMEOUT (%s)", c.Consumer.ProcessTimeout, c.Queues.VisibilityTimeout)
	}
	if c.Queues.MaxReceiveCount < 1 {
		add("SQS_MAX_RECEIVE_COUNT must be >= 1")
	}
	for name, q := range map[string]string{"SQS_INPUT_QUEUE": c.Queues.Input, "SQS_INPUT_DLQ": c.Queues.InputDLQ, "SQS_EVENTS_QUEUE": c.Queues.Events} {
		if !strings.HasSuffix(q, ".fifo") {
			add("%s must be a FIFO queue name ending in .fifo", name)
		}
	}
	if c.Pending.MaxAttempts < 1 || c.Pending.TTL <= 0 || c.Pending.BackoffBase <= 0 || c.Pending.Lease <= 0 {
		add("PENDING_* settings must be positive")
	}
	if c.Outbox.BatchSize < 1 || c.Outbox.Lease <= 0 || c.Outbox.PollInterval <= 0 {
		add("OUTBOX_* settings must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		add("SHUTDOWN_TIMEOUT must be positive")
	}
	return errors.Join(errs...)
}
