# Convenience targets. Every target is a thin wrapper over a documented command.
GO ?= go
COMPOSE ?= docker compose

.PHONY: build fmt vet test test-race up down deps deps-down migrate-up migrate-down queues-init \
        test-integration test-all-in-docker

build:
	$(GO) build -o bin/wagering ./cmd/wagering

fmt:
	gofmt -l -w cmd internal migrations test

vet:
	$(GO) vet ./...
	$(GO) vet -tags integration ./test/...

# Unit tests only (no infrastructure needed).
test:
	$(GO) test ./...

# Race detector (needs cgo/gcc; use test-all-in-docker if your host has no C compiler).
test-race:
	CGO_ENABLED=1 $(GO) test -race ./...

# Full stack: three service instances + PostgreSQL + LocalStack + Keycloak.
up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down -v

# Only the dependencies needed by the integration tests.
deps:
	$(COMPOSE) up -d postgres localstack keycloak

deps-down:
	$(COMPOSE) down -v

migrate-up:
	$(GO) run ./cmd/wagering migrate up

migrate-down:
	$(GO) run ./cmd/wagering migrate down

queues-init:
	$(GO) run ./cmd/wagering queues init

# Integration tests against the containers started by `make deps`.
test-integration:
	$(GO) test -tags integration -count=1 -timeout 25m ./test/integration/...

# Unit + integration tests with -race inside Docker (no host toolchain needed).
test-all-in-docker:
	$(COMPOSE) --profile tests run --rm tests
