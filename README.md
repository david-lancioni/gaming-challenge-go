# Processamento distribuído de apostas em Go

Serviço em **Go 1.26** com **Uber Fx** que movimenta carteiras de jogadores a partir de
operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), recebidas por
**HTTP** e por **AWS SQS**, com PostgreSQL como fonte única de verdade, idempotência
persistente, ledger append-only, *inbox/outbox* e autenticação OAuth 2.0/OIDC (Keycloak).

- Enunciado original: [CHALLENGE.md](CHALLENGE.md)
- Decisões, contratos, máquina de estados, limitações: [ARCHITECTURE.md](ARCHITECTURE.md)

## Sumário

1. [Pré-requisitos](#pré-requisitos)
2. [Início rápido](#início-rápido)
3. [Variáveis de ambiente](#variáveis-de-ambiente)
4. [Migrations](#migrations)
5. [Filas SQS](#filas-sqs)
6. [Executando o serviço fora do Docker](#executando-o-serviço-fora-do-docker)
7. [Autenticação e identidades de teste](#autenticação-e-identidades-de-teste)
8. [Exemplos de chamadas](#exemplos-de-chamadas)
9. [Mensagens SQS de exemplo](#mensagens-sqs-de-exemplo)
10. [Testes](#testes)
11. [Simulações de falha](#simulações-de-falha)
12. [Observabilidade](#observabilidade)
13. [Estrutura do repositório](#estrutura-do-repositório)
14. [Solução de problemas](#solução-de-problemas)

## Pré-requisitos

| Necessário para | Ferramenta |
| --- | --- |
| Tudo em containers | Docker 24+ com Compose v2 |
| Compilar/testar no host | Go **1.26** (a versão está em `go.mod` e no `Dockerfile`) |
| `go test -race` no host | compilador C (cgo). Sem ele, use `make test-all-in-docker` |

Não é preciso instalar mais nada: PostgreSQL, LocalStack (SQS) e Keycloak rodam em containers.

## Início rápido

```sh
docker compose up --build
```

Sobe PostgreSQL 17, LocalStack (SQS), Keycloak (com o realm `wagering` importado
automaticamente), um passo único de bootstrap (**aplica as migrations e provisiona as filas**)
e **três instâncias independentes** do serviço:

| Serviço | API | Métricas |
| --- | --- | --- |
| `api-1` | http://localhost:8081 | http://localhost:9101/metrics |
| `api-2` | http://localhost:8082 | http://localhost:9102/metrics |
| `api-3` | http://localhost:8083 | http://localhost:9103/metrics |
| Keycloak | http://localhost:8080 (admin/admin) | |
| PostgreSQL | `localhost:5432` (wagering/wagering) | |
| LocalStack | http://localhost:4566 | |

As três instâncias compartilham apenas o banco e as filas; qualquer uma atende HTTP, consome
SQS, publica o outbox e retoma pendências. Verifique:

```sh
curl -s localhost:8081/health/ready    # {"checks":{"postgres":"UP","sqs":"UP"},"status":"UP"}
```

Para parar e limpar: `docker compose down -v`.

## Variáveis de ambiente

Todas têm valor padrão para o ambiente local; copie [.env.example](.env.example) para `.env`
se quiser alterá-las (o Compose o lê automaticamente). **Não há segredos reais** nele.
As principais:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `DATABASE_URL` | — (obrigatória) | conexão PostgreSQL |
| `OIDC_ISSUER` | — (obrigatória com HTTP) | `iss` esperado, URL pública do realm |
| `OIDC_JWKS_URL` | — (obrigatória com HTTP) | onde buscar as chaves de assinatura |
| `OIDC_AUDIENCE` / `OIDC_ROLE_PROVIDER` / `OIDC_ROLE_INTERNAL` / `OIDC_PROVIDER_CLAIM` | `wagering-api` / `wagering-provider` / `wagering-internal` / `provider_id` | modelo de permissões |
| `AWS_ENDPOINT_URL`, `AWS_REGION` | — / `us-east-1` | endpoint do SQS (LocalStack) |
| `SQS_INPUT_QUEUE`, `SQS_INPUT_DLQ`, `SQS_EVENTS_QUEUE` | `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wager-events.fifo` | filas (FIFO) |
| `SQS_VISIBILITY_TIMEOUT`, `SQS_MAX_RECEIVE_COUNT` | `60s`, `5` | visibilidade e redrive |
| `HTTP_ADDR`, `METRICS_ADDR` | `:8080`, `:9100` | listeners |
| `ENABLE_HTTP`, `ENABLE_CONSUMER`, `ENABLE_PUBLISHER`, `ENABLE_PENDING_WORKER` | `true` | papéis desta instância |
| `PENDING_TTL`, `PENDING_MAX_ATTEMPTS`, `PENDING_BACKOFF_*` | `10m`, `10`, `1s…60s` | espera por referências |
| `CONSUMER_*`, `OUTBOX_*` | ver `.env.example` | concorrência, backoff, lease |
| `SHUTDOWN_TIMEOUT` | `25s` | prazo do encerramento gracioso |
| `DB_LOCK_TIMEOUT`, `DB_STATEMENT_TIMEOUT` | `5s`, `15s` | limites por sessão |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

A configuração é validada na inicialização (por exemplo, `CONSUMER_PROCESS_TIMEOUT` deve ser
menor que `SQS_VISIBILITY_TIMEOUT`); erro de configuração impede o início.

## Migrations

Arquivos versionados em [`migrations/`](migrations) (`NNNN_nome.up.sql` / `.down.sql`,
compatíveis com a CLI `golang-migrate`). O binário traz um executor próprio:

```sh
export DATABASE_URL='postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable'

go run ./cmd/wagering migrate up        # aplica as pendentes (idempotente; lock consultivo)
go run ./cmd/wagering migrate status    # lista as aplicadas
go run ./cmd/wagering migrate down      # REVERTE a última (destrói dados — só dev/teste)
go run ./cmd/wagering migrate down 2    # reverte as 2 últimas
```

No Compose, o serviço `bootstrap` executa `migrate up && queues init` antes das instâncias.
Para reverter no Compose: `docker compose run --rm bootstrap "wagering migrate down"`.

## Filas SQS

`wagering queues init` (idempotente) cria `wager-transactions.fifo`, a DLQ
`wager-transactions-dlq.fifo` com **redrive** (`maxReceiveCount` = 5) e
`wager-events.fifo` (destino dos eventos), além das políticas de acesso por fila:

```sh
AWS_ENDPOINT_URL=http://localhost:4566 go run ./cmd/wagering queues init
```

O `MessageGroupId` das mensagens de entrada deve ser o `walletId` e o
`MessageDeduplicationId` o `messageId` (detalhes e limites em [ARCHITECTURE.md §10](ARCHITECTURE.md#10-inbox-consumidor-sqs-e-dlq)).

## Executando o serviço fora do Docker

Suba só as dependências e o serviço no host:

```sh
docker compose up -d postgres localstack keycloak

export DATABASE_URL='postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable'
export AWS_ENDPOINT_URL=http://localhost:4566 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export OIDC_ISSUER=http://localhost:8080/realms/wagering
export OIDC_JWKS_URL=http://localhost:8080/realms/wagering/protocol/openid-connect/certs

go run ./cmd/wagering migrate up
go run ./cmd/wagering queues init
HTTP_ADDR=:8081 METRICS_ADDR=:9101 go run ./cmd/wagering serve      # instância 1
HTTP_ADDR=:8082 METRICS_ADDR=:9102 go run ./cmd/wagering serve      # instância 2 (outro terminal)
```

(PowerShell: `$env:DATABASE_URL='...'` etc.)

## Autenticação e identidades de teste

O realm `wagering` é provisionado por [`deploy/keycloak/wagering-realm.json`](deploy/keycloak/wagering-realm.json)
(clientes com `client_credentials`; papel e `provider_id` vêm do token validado):

| client_id | secret | papel | efeito |
| --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider`, `provider_id=provider-a` | envia/consulta as **suas** operações |
| `provider-b` | `provider-b-secret` | `wagering-provider`, `provider_id=provider-b` | idem, isolado do `provider-a` |
| `wallet-service` | `wallet-service-secret` | `wagering-internal` | carteiras, ledger, reconciliação, leituras de qualquer provedor |
| `no-role-client` | `no-role-client-secret` | — | token válido, sem permissão (`403`) |
| `wrong-audience-client` | `wrong-audience-client-secret` | provider | audiência errada (`401`) |
| `expiring-provider` | `expiring-provider-secret` | provider | token de 1 s (teste de expiração) |

Obtendo tokens:

```sh
tok() { curl -s http://localhost:8080/realms/wagering/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id="$1" -d client_secret="$1-secret" | jq -r .access_token; }
INTERNAL=$(tok wallet-service)
PROVIDER=$(tok provider-a)
```

```powershell
function tok($c) { (Invoke-RestMethod -Method Post -Uri http://localhost:8080/realms/wagering/protocol/openid-connect/token `
  -Body @{grant_type='client_credentials'; client_id=$c; client_secret="$c-secret"}).access_token }
$INTERNAL = tok 'wallet-service'; $PROVIDER = tok 'provider-a'
```

Sem token → `401`; com token sem permissão → `403`. Os tokens duram 5 minutos.

## Exemplos de chamadas

```sh
API=http://localhost:8081
PLAYER=$(uuidgen | tr A-Z a-z)

# 1. Abrir carteira (serviço interno) — 201
curl -s -X POST $API/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
WALLET=<id retornado>

# 2. Enviar uma aposta (provedor) — 200 {"status":"PROCESSED","balance":{"amount":"975.00",...}}
curl -s -X POST $API/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# 3. Repetir exatamente a mesma chamada → 200 com "idempotentReplay": true (mesmo saldo original)
# 4. Mesma chave com valor diferente → 409 IDEMPOTENCY_KEY_CONFLICT

# 5. Reembolso — 200 (ou 202 PENDING_REFERENCE se a aposta ainda não chegou)
curl -s -X POST $API/wagering/transactions -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: provider-a:refund-1' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"refund-1\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"REFUND\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"transaction-123\"}"

# 6. Consultas
curl -s $API/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
curl -s $API/wagering/transactions/<transactionId>                     -H "Authorization: Bearer $PROVIDER"
curl -s "$API/wallets/$WALLET/ledger?limit=50"                          -H "Authorization: Bearer $INTERNAL"
curl -s "$API/wallets/$WALLET/ledger?limit=50&cursor=<nextCursor>"      -H "Authorization: Bearer $INTERNAL"
curl -s -X POST $API/wallets/$WALLET/reconciliation                     -H "Authorization: Bearer $INTERNAL"
```

Códigos HTTP e corpos de cada situação (entrada inválida, conflito, rejeição de negócio,
pendência, indisponibilidade transitória): [ARCHITECTURE.md §12](ARCHITECTURE.md#12-contrato-http).

### Postman

[`postman/wagering.postman_collection.json`](postman/wagering.postman_collection.json) traz o
mesmo roteiro pronto: importe no Postman (**Import**) e envie as requisições **em ordem**,
começando por `1. Abrir carteira`. Os tokens do Keycloak são obtidos e renovados pelo script da
collection; `playerId`, `walletId` e os ids das operações ficam em variáveis da collection.

Cada execução de `1. Abrir carteira` cria um jogador novo, então o roteiro pode ser repetido do
início. As requisições cobrem aposta, replay idempotente, conflito de chave (`409`), `WIN`,
`LOSS`, `REFUND`, carteira, ledger, reconciliação, consultas de transação e o isolamento entre
provedores. Para usar outra instância, altere a variável `baseUrl` (`http://localhost:8082`).

Pela linha de comando, sem instalar Node, com o [Newman](https://github.com/postmanlabs/newman)
em container na rede do Compose:

```sh
docker run --rm --network backend-challenge-go_default -v "$PWD/postman:/etc/newman" \
  postman/newman:alpine run wagering.postman_collection.json \
  --env-var baseUrl=http://api-1:8080 --env-var keycloakUrl=http://keycloak:8080
```

## Mensagens SQS de exemplo

```sh
Q=http://localhost:4566/000000000000/wager-transactions.fifo
aws --endpoint-url http://localhost:4566 sqs send-message --queue-url $Q \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123",
            "playerId":"'$PLAYER'","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
            "money":{"amount":"25.00","currency":"BRL"}}}'
```

Mensagens inválidas (JSON quebrado, dinheiro inválido, `OPENING`, conflitos de idempotência)
vão para `wager-transactions-dlq.fifo` com os atributos `dlqReason`/`dlqError`.
Os eventos de saída ficam em `wager-events.fifo` (`eventType`/`eventId` como atributos).

## Testes

```sh
go test ./...          # unitários (sem infraestrutura): domínio, casos de uso, HTTP, auth, config...
go test -race ./...    # com detector de corridas (requer cgo/gcc; veja abaixo se faltar)
go vet ./...
gofmt -l cmd internal migrations test    # não deve listar nada
```

### Integração, múltiplas instâncias e simulações de falha

Os testes de integração usam **PostgreSQL, Keycloak e LocalStack reais** (containers) e sobem
**processos independentes** do serviço. Ficam atrás da *build tag* `integration`.

```sh
# 1. dependências
docker compose up -d postgres localstack keycloak      # aguarde ficarem "healthy"

# 2. suíte completa (~3–4 min)
go test -tags integration -count=1 -timeout 25m ./test/integration/...

# 3. um cenário específico
go test -tags integration -count=1 -v ./test/integration/ -run 'TestTwoBetsOf80OnABalanceOf100'
go test -tags integration -count=1 -v ./test/integration/ -run 'Crash|Restart|Graceful|Outage'

# 4. com o detector de corridas
CGO_ENABLED=1 go test -race -tags integration -count=1 -timeout 25m ./test/integration/...
```

Sem compilador C no host (ex.: Windows), rode **tudo** — unitários e integração com `-race` —
dentro do Compose, sem instalar nada:

```sh
docker compose --profile tests run --rm tests        # ou: make test-all-in-docker
```

Cada teste cria seu próprio banco (`it_*`) e suas filas, então os testes não interferem entre si
nem com o ambiente de `docker compose up`. Variáveis opcionais: `TEST_ADMIN_DATABASE_URL`,
`TEST_AWS_ENDPOINT_URL`, `TEST_KEYCLOAK_URL`, `TEST_OIDC_ISSUER`.

| Requisito do desafio (§13) | Testes |
| --- | --- |
| 50 duplicatas em paralelo → 1 débito | `TestSameBetFiftyTimesInParallelAcrossThreeProcesses` |
| 2 × 80,00 sobre 100,00 | `TestTwoBetsOf80OnABalanceOf100` (+ `internal/application`) |
| carteiras distintas em paralelo | `TestManyWalletsConcurrentlyAcrossThreeProcesses`, `TestIndependentWalletsAreNotBlockedByAnotherWalletsLock` |
| 3 instâncias independentes | todos os de `concurrency_test.go` (`cluster(t, 3, …)`) |
| crash depois do commit / antes do delete | `TestConsumerCrashAfterCommitBeforeDelete` |
| dois publishers, recuperação | `TestTwoPublishersCompeteForTheSameOutbox`, `TestPublisherCrashBetweenPublishAndConfirmation`, `TestOutboxSurvivesSQSOutage`, `TestOutboxClaimKeepsPerWalletOrder` |
| referência tardia / expiração | `TestPendingReference*`, `TestReferenceThatEndsWithoutSuccess`, `TestReversalBeforeReferenceThroughSQS` |
| reinício e pendência retomada por outra instância | `TestRestartPreservesIdempotencyPendingAndConsistency`, `TestCommittedPendingOperationsAreResumedByAnotherInstance` |
| HTTP × SQS para a mesma operação | `TestSameOperationThroughHTTPAndSQSConcurrently`, `TestSQSConsumerProcessesAndDeduplicates` |
| Fx: composição, início/encerramento, liberação de workers | `TestFxCompositionStartsAndReleasesEveryResource`, `TestGracefulShutdownCompletesInFlightWork`, `TestShutdownDeadlineReleasesInFlightMessages`, `internal/app` (`fx.ValidateApp`) |
| autenticação real, isolamento de provedores | `TestAuthentication`, `TestProviderIsolation`, `TestInternalEndpointsAreRestrictedToTheInternalService` |
| migrations, constraints, ledger imutável | `TestMigrationsApplyAndRevert`, `TestSchema*` |
| conferência final saldo × ledger | `assertConsistent` em todos os cenários + `TestRandomWorkloadKeepsFinancialInvariants` |

## Simulações de falha

Além dos testes, é possível reproduzir manualmente. A variável **`FAULT_INJECTION`**
(somente para testes; nunca em produção) encerra o processo de forma abrupta em pontos exatos:

| Valor | Efeito |
| --- | --- |
| `consumer_after_commit` | sai (código 137) depois do commit da mensagem e antes de removê-la da fila |
| `publisher_after_send` | sai depois de enviar um evento ao SQS e antes de marcá-lo como publicado |

```sh
FAULT_INJECTION=consumer_after_commit go run ./cmd/wagering serve   # envie uma mensagem: o processo morre após o commit
# suba outra instância: a mensagem é reentregue após o visibility timeout e confirmada pela inbox, sem novo débito
```

Outras simulações sem código extra: `docker compose kill api-1` (SIGKILL) durante carga;
`docker compose stop postgres` / `localstack` (o serviço responde `503` / o outbox acumula e
recupera); `docker compose stop -t 30 api-2` (SIGTERM: drena o trabalho em andamento).

## Observabilidade

- Logs JSON por instância (`docker compose logs api-1`), com `correlationId`, `messageId`,
  `transactionId`, `walletId`, `providerId`. Envie `X-Correlation-Id` para rastrear uma chamada.
- Métricas Prometheus em `:9100/metrics` de cada instância (lista em
  [ARCHITECTURE.md §15](ARCHITECTURE.md#15-observabilidade)):
  `curl -s localhost:9101/metrics | grep ^wagering_`
- Health: `/health/live` e `/health/ready` (PostgreSQL + SQS).

## Estrutura do repositório

```
cmd/wagering/            binário: serve | migrate | queues
internal/domain/         Money, Wallet, LedgerEntry, WagerTransaction, eventos (puro Go)
internal/application/    Processor, serviços, portas (interfaces)
internal/infra/postgres/ pgx: UnitOfWork, repositórios, filas de outbox/pendências, migrator
internal/infra/messaging/ SQS: provisionamento, consumidor, publisher
internal/infra/httpapi/  handlers, contrato de erros
internal/infra/auth/     validação OIDC/JWT
internal/infra/worker/   laço do worker de pendências
internal/app/            composição com Uber Fx
internal/observability/  logs e métricas
internal/testutil/memory repositórios em memória para testes unitários
migrations/              SQL versionado (up/down) embutido no binário
deploy/keycloak/         realm importado automaticamente
postman/                 collection do Postman com o roteiro de chamadas
test/integration/        testes com PostgreSQL, Keycloak, LocalStack e processos reais
```

## Solução de problemas

- **`go test -race` falha com "-race requires cgo"**: instale um compilador C ou use
  `docker compose --profile tests run --rm tests`.
- **Porta em uso** (5432, 8080, 4566): pare o serviço local ou altere os mapeamentos no Compose.
- **Testes de integração dizem "dependencies are not available"**: rode
  `docker compose up -d postgres localstack keycloak` e aguarde `healthy`
  (`docker compose ps`); o Keycloak leva ~20–40 s na primeira subida.
- **Relógio do Docker desatualizado** (Docker Desktop após suspender o computador): containers
  ficam com hora diferente do host; tokens podem expirar "cedo" e o LocalStack pode demorar a
  reentregar mensagens FIFO. O serviço continua correto, mas testes que dependem de
  reentrega/lease podem oscilar. Reinicie o Docker/WSL (`wsl --shutdown`) para ressincronizar.
- **`401` em todas as chamadas**: confira `OIDC_ISSUER` (deve ser exatamente o `iss` do token:
  `http://localhost:8080/realms/wagering`) e a hora do host.
