package config

import (
	"strings"
	"testing"
	"time"
)

func validEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("OIDC_ISSUER", "http://localhost:8080/realms/wagering")
	t.Setenv("OIDC_JWKS_URL", "http://keycloak:8080/realms/wagering/protocol/openid-connect/certs")
}

func TestLoadAppliesDefaults(t *testing.T) {
	validEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.Queues.Input != "wager-transactions.fifo" || cfg.Queues.InputDLQ != "wager-transactions-dlq.fifo" ||
		cfg.Queues.Events != "wager-events.fifo" || cfg.Queues.MaxReceiveCount != 5 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if !cfg.EnableHTTP || !cfg.EnableConsumer || !cfg.EnablePublisher || !cfg.EnablePendingWorker {
		t.Fatal("every component is enabled by default: any instance must be able to run everything")
	}
	if cfg.Consumer.ProcessTimeout >= cfg.Queues.VisibilityTimeout {
		t.Fatal("the processing timeout must stay below the visibility timeout")
	}
	if cfg.InstanceID == "" {
		t.Fatal("an instance id is generated")
	}
}

func TestLoadReadsOverrides(t *testing.T) {
	validEnv(t)
	t.Setenv("SQS_MAX_RECEIVE_COUNT", "9")
	t.Setenv("PENDING_TTL", "3m")
	t.Setenv("ENABLE_CONSUMER", "false")
	t.Setenv("INSTANCE_ID", "node-7")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Queues.MaxReceiveCount != 9 || cfg.Pending.TTL != 3*time.Minute || cfg.EnableConsumer || cfg.InstanceID != "node-7" {
		t.Fatalf("overrides ignored: %+v", cfg)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	cases := map[string]struct {
		env  map[string]string
		want string
	}{
		"bad duration":             {map[string]string{"PENDING_TTL": "soon"}, "PENDING_TTL"},
		"bad bool":                 {map[string]string{"ENABLE_HTTP": "maybe"}, "ENABLE_HTTP"},
		"bad integer":              {map[string]string{"DB_MAX_CONNS": "many"}, "DB_MAX_CONNS"},
		"non-fifo queue":           {map[string]string{"SQS_INPUT_QUEUE": "wager-transactions"}, "SQS_INPUT_QUEUE"},
		"timeout above visibility": {map[string]string{"CONSUMER_PROCESS_TIMEOUT": "2m"}, "CONSUMER_PROCESS_TIMEOUT"},
		"batch too large":          {map[string]string{"CONSUMER_BATCH_SIZE": "11"}, "CONSUMER_BATCH_SIZE"},
		"zero receive count":       {map[string]string{"SQS_MAX_RECEIVE_COUNT": "0"}, "SQS_MAX_RECEIVE_COUNT"},
		"relative jwks url":        {map[string]string{"OIDC_JWKS_URL": "/certs"}, "OIDC_JWKS_URL"},
		"non-positive pending":     {map[string]string{"PENDING_TTL": "0s"}, "PENDING_"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			validEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want mention of %s", err, tc.want)
			}
		})
	}
}

func TestDatabaseAndAuthenticationAreMandatory(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OIDC_ISSUER", "")
	t.Setenv("OIDC_JWKS_URL", "")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") || !strings.Contains(err.Error(), "OIDC_ISSUER") {
		t.Fatalf("error = %v", err)
	}
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("ENABLE_HTTP", "false")
	if _, err := Load(); err != nil {
		t.Fatalf("worker-only configuration: %v", err)
	}
}

func TestLoadUnvalidatedForAdministrativeCommands(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("OIDC_ISSUER", "")
	cfg, err := LoadUnvalidated()
	if err != nil || cfg.Database.URL == "" {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}
