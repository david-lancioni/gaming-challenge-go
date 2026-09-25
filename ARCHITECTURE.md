# ARCHITECTURE

Decisões técnicas, contratos e limitações da solução do [desafio](CHALLENGE.md).
Para executar o projeto, veja o [README](README.md).

## Sumário

1. [Visão geral](#1-visão-geral)
2. [Dinheiro (`Money`)](#2-dinheiro-money)
3. [Persistência, transações SQL e mapeamento](#3-persistência-transações-sql-e-mapeamento)
4. [Concorrência e locks](#4-concorrência-e-locks)
5. [Máquina de estados de `WagerTransaction`](#5-máquina-de-estados-de-wagertransaction)
6. [Idempotência](#6-idempotência)
7. [Referências pendentes](#7-referências-pendentes)
8. [Reversões (`REFUND` e `ROLLBACK`)](#8-reversões-refund-e-rollback)
9. [Ledger e proteções do banco](#9-ledger-e-proteções-do-banco)
10. [Inbox, consumidor SQS e DLQ](#10-inbox-consumidor-sqs-e-dlq)
11. [Outbox e publicação de eventos](#11-outbox-e-publicação-de-eventos)
12. [Contrato HTTP](#12-contrato-http)
13. [Autenticação e autorização](#13-autenticação-e-autorização)
14. [Uber Fx, ciclo de vida e shutdown](#14-uber-fx-ciclo-de-vida-e-shutdown)
15. [Observabilidade](#15-observabilidade)
16. [Estratégia de testes](#16-estratégia-de-testes)
17. [Interpretações adotadas](#17-interpretações-adotadas)
18. [Limitações e trabalho não concluído](#18-limitações-e-trabalho-não-concluído)

---

## 1. Visão geral

```
                 ┌───────────── IdP (Keycloak, OIDC) ─────────────┐
                 │  client_credentials → JWT (JWKS, RS256)        │
                 └────────────────────────────────────────────────┘
   provedores ── HTTP ──►┐
                         │      ┌──────────── mesmo caso de uso: application.Processor ───────────┐
   SQS FIFO ── consumer ─┼────► │ trava a carteira (FOR UPDATE) → idempotência → regras → ledger │
   (wager-transactions)  │      │ saldo + ledger + estado + inbox + outbox: UMA transação SQL    │
   worker de pendências ─┘      └──────────────────────────────┬─────────────────────────────────┘
                                                               ▼
                                                      PostgreSQL (fonte única de verdade)
                                                               ▲
   publisher(s) do outbox ── SKIP LOCKED + lease ──────────────┘
        └──► SQS FIFO wager-events.fifo (MessageGroupId = carteira, MessageDeduplicationId = eventId)
```

- Todo estado durável está no PostgreSQL; **nenhuma instância guarda estado que
  as outras precisem**. Qualquer instância executa qualquer papel (API, consumidor,
  publisher, worker de pendências); cada papel pode ser desligado por variável
  de ambiente (`ENABLE_*`).
- Camadas (dependências apontam para dentro):
  `domain` (puro: sem Fx, HTTP, SQS ou driver) ← `application` (casos de uso e
  portas) ← `infra/*` (postgres, messaging, httpapi, auth, worker) ← `app` (Fx) ← `cmd`.
- HTTP, SQS e o worker de pendências chamam o **mesmo** `application.Processor`
  dentro de uma transação SQL que eles próprios abrem; por isso têm exatamente as
  mesmas garantias.

## 2. Dinheiro (`Money`)

| Aspecto | Decisão |
| --- | --- |
| Representação | `int64` em **centavos** + código de moeda. Escala fixa de 2 casas. |
| Limites | `-92233720368547758.08` a `92233720368547758.07`. Parsing, soma, subtração e negação verificam overflow (`ErrMoneyOverflow`). |
| Entrada externa | Gramática única: `0\|[1-9][0-9]*` `.` `[0-9]{2}`. Rejeita vazio, `NaN`, `Infinity`, notação científica, sinal `+`, espaços, separador de milhar, zeros à esquerda, **menos ou mais de 2 casas** e negativos (`ErrNegativeAmount`). |
| Normalização | **Nenhuma**: só existe uma grafia aceita por valor, portanto não há formas equivalentes a normalizar antes do hash. Nada é arredondado. |
| JSON | `{"amount":"25.00","currency":"BRL"}`. `amount` é sempre *string*: um número JSON é rejeitado antes de qualquer conversão, então nunca há `float`. |
| Moeda | ISO 4217 em maiúsculas, restrita às moedas com expoente 2 (BRL, USD, EUR, GBP, ARS, MXN, ...; lista em `domain/money.go`). Aritmética e comparação exigem moedas iguais (`ErrCurrencyMismatch`). |
| Valor zero | `Money{}` (não inicializado) é rejeitado por toda operação. Zero é aceito no saldo inicial e em `LOSS`; `BET/WIN/REFUND/ROLLBACK` exigem valor > 0. |
| Negativos | Permitidos em diferenças/cálculos internos (ex.: `difference` da reconciliação), nunca no saldo (CHECK no banco) nem em entradas externas. |
| Persistência | `BIGINT` (centavos) + `CHAR(3)`; leitura e escrita exatas, sem `NUMERIC`/float. |

Não há `float32`/`float64` em nenhum ponto do fluxo monetário (o único `float64` do
projeto é a duração do atraso do outbox, em segundos, para métricas).

## 3. Persistência, transações SQL e mapeamento

- **Biblioteca**: `pgx/v5` + `pgxpool` com **SQL explícito** (`internal/infra/postgres`).
- **Mapeamento de `Money`**: `amount_minor BIGINT` + `currency CHAR(3)`
  (`Money.Minor()` / `Money.Currency()` ↔ `MoneyFromMinor`).
- **Delimitação da transação**: `UnitOfWork.Do(ctx, fn)` abre **uma** transação
  `READ COMMITTED`, entrega aos repositórios o mesmo `pgx.Tx` e faz `COMMIT` se
  `fn` retornar `nil`, `ROLLBACK` caso contrário. Repositórios nunca abrem,
  confirmam ou revertem transações; quem decide a fronteira é o caso de uso.
  `fn` é reexecutada (até 3 vezes, com backoff curto) em *deadlock* (`40P01`) e
  falha de serialização (`40001`), portanto reconstrói seus agregados a cada execução.
- Timeouts de sessão por conexão: `lock_timeout` (5 s), `statement_timeout` (15 s) e
  `idle_in_transaction_session_timeout` (30 s), este último para que um cliente
  travado não segure locks de carteira indefinidamente.
- **Classificação de erros** (`mapErr`): conexão, timeout, cancelamento, `55P03`
  (lock timeout), `40001/40P01`, classes `08`, `53`, `57P` → `application.ErrTransient`
  (a causa original é preservada). Violações de constraint e demais erros são
  permanentes.
- **Migrations** (`migrations/NNNN_nome.{up,down}.sql`, formato compatível com
  `golang-migrate`): executor próprio e pequeno (`wagering migrate up|down [N]|status`),
  com lock consultivo (`pg_advisory_lock`) para que várias instâncias iniciando juntas
  não apliquem a mesma migration duas vezes, e uma transação por migration.
- **Fonte única de tempo**: quem decide se algo está "devido" (retry de pendência,
  lease de outbox/pendência) é o **relógio do banco** (`now()`). O domínio calcula o
  próximo retry como *atraso* relativo ao instante da transição e o repositório o
  converte para `now() + atraso` (`nextAttemptDelay`). Assim, desvio de relógio entre
  instâncias e banco não atrasa nem antecipa retries.

## 4. Concorrência e locks

Estratégia: **lock pessimista por carteira** (`SELECT ... FOR UPDATE` na linha de
`wallets`) combinado com controle **otimista** e restrições do banco como defesa em
profundidade.

1. Toda transação SQL que altera uma carteira ou as transações dela (HTTP, SQS,
   worker de pendências) **adquire primeiro o lock da carteira**. Há uma única
   ordem de aquisição (carteira → linhas de transação), logo não há *deadlock*
   entre esses caminhos. Carteiras diferentes nunca disputam: **não há lock global**.
2. `UPDATE wallets ... WHERE id = $1 AND version = $expected` — se a versão mudou,
   `ErrConcurrentModification` (nenhuma atualização confirmada é descartada).
3. O banco também protege sozinho (independente de locks locais): `UNIQUE
   (wallet_id, wallet_version)` no ledger, trigger que exige que cada lançamento
   continue a cadeia (`balance_before` = `balance_after` anterior; versão + 1), e
   *constraint trigger* adiado que, no `COMMIT`, exige `saldo/versão da carteira =
   último lançamento` (§9).
4. Replays são atendidos por um caminho rápido **sem** lock (consulta por chave); a
   verificação autoritativa é refeita sob o lock.
5. Corrida de inserção da mesma chave (por exemplo, duas carteiras diferentes com a
   mesma chave/`externalTransactionId`): a violação de unicidade é convertida em
   `ErrDuplicateTransaction` e o caso de uso reavalia do zero (até 3 vezes).

Teste obrigatório do desafio (`TestTwoBetsOf80OnABalanceOf100`): duas apostas de
80,00 sobre 100,00, de processos distintos, resultam em uma `PROCESSED`, uma
`REJECTED/INSUFFICIENT_FUNDS`, saldo 20,00 e um único débito — repetido 12 vezes com
reenvios.

## 5. Máquina de estados de `WagerTransaction`

```
                ┌────────────── WaitForReference ──────────────┐
                │                                              ▼
 (criação) → PENDING ───────────────────────────────► PENDING_REFERENCE ─┐ RetryReference
                │                                              │  ▲──────┘ (attempts++)
                ├──────── MarkProcessed / Reject / Fail ───────┤
                ▼                                              ▼
        PROCESSED | REJECTED | FAILED   (terminais: nenhuma transição, nem no banco)
```

- As transições são métodos do agregado (`WaitForReference`, `RetryReference`,
  `MarkProcessed`, `Reject`, `Fail`), validadas pelo domínio, e cada uma registra os
  eventos correspondentes. Um agregado **reidratado** não reaplica nada e não emite
  eventos (`RehydrateTransaction`).
- O banco repete a regra: um trigger recusa qualquer `UPDATE` de linha terminal e
  o retorno `PENDING_REFERENCE → PENDING`.
- **Caminho síncrono**: uma operação sem dependência pendente é decidida e confirmada
  numa única transação, já no estado final (sem *commit* intermediário de aceite). Por
  isso o estado `PENDING` nunca é confirmado pelo caminho normal; ainda assim, todo
  `PENDING` confirmado é retomado por qualquer instância (o worker consulta
  `PENDING` e `PENDING_REFERENCE`; há teste que insere um `PENDING` direto no banco
  e outra instância o conclui).
- **Falha transitória × permanente**:
  - transitória (`ErrTransient`): nada é gravado e a operação é repetida — HTTP `503`
    com `Retry-After`, SQS com backoff de visibilidade, worker com lease que expira;
  - permanente: entrada inválida (`400` / DLQ), regra de negócio (persistida como
    `REJECTED`, terminal) ou erro inesperado. Um erro permanente ao retomar uma operação
    pendente a fecha como `FAILED` (`PROCESSING_ERROR`) para auditoria.

## 6. Idempotência

Persistente e independente de memória: restrições `UNIQUE (provider_id, idempotency_key)`
e `UNIQUE (provider_id, external_transaction_id)`.

**Hash do payload** (`domain.ComputePayloadHash`, igual em HTTP e SQS): SHA-256 (hex)
do JSON canônico — objeto plano com chaves ordenadas lexicograficamente, UTF-8, sem
espaços, sem *escape* de HTML — dos campos de negócio:
`providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind,
amount, currency` e `referenceExternalTransactionId` (omitido se ausente). UUIDs são
canônicos (minúsculos, hifenizados). **Ficam de fora**: a chave de idempotência,
`messageId`, `occurredAt`, cabeçalhos e ids de correlação. Não há outra normalização
porque as entradas são validadas de forma estrita (nenhuma grafia alternativa).

| Situação | Resultado |
| --- | --- |
| Mesma chave, mesmo hash | Replay: devolve o resultado persistido, `idempotentReplay: true`. Para operação concluída, o **saldo observado no processamento original** (`result_balance_minor`). |
| Mesma chave, hash diferente | `409 IDEMPOTENCY_KEY_CONFLICT` |
| Mesmo `(providerId, externalTransactionId)` com outra chave | `409 EXTERNAL_TRANSACTION_CONFLICT` (a operação financeira não pode ser reaplicada com outra chave) |
| Sem `Idempotency-Key` | `400` |

O servidor nunca substitui a chave recebida: ela é gravada como veio (teste
`TestIdempotencyContract`). Rejeições de negócio também são idempotentes: o replay
devolve a rejeição original mesmo que a carteira tenha sido recarregada depois.

## 7. Referências pendentes

`REFUND`, `ROLLBACK` (e `WIN` que informa referência) resolvem a referência por
`(providerId, referenceExternalTransactionId)`.

| Situação da referência | Comportamento |
| --- | --- |
| Não existe (ainda) | Persiste `PENDING_REFERENCE`, emite `WagerTransactionPendingReference` e responde `202`. |
| Existe, mas está `PENDING`/`PENDING_REFERENCE` | Continua aguardando (mesma política). Se o orçamento acabar: `REJECTED / REFERENCE_UNAVAILABLE`. |
| Existe e terminou `REJECTED`/`FAILED` | `REJECTED / REFERENCE_NOT_PROCESSED` imediatamente: ela nunca terá efeito a reverter. |
| Existe e `PROCESSED` | Valida provedor, jogador, carteira, moeda, rodada, tipo e valor; aplica. |
| Nunca chegou e o orçamento acabou | `REJECTED / REFERENCE_NOT_FOUND` + evento `WagerTransactionRejected`. |

- **Orçamento**: `PENDING_MAX_ATTEMPTS` (padrão 10) tentativas **ou** `PENDING_TTL`
  (padrão 10 min), o que vier primeiro. Backoff exponencial com jitter
  (`PENDING_BACKOFF_BASE`=1 s … `PENDING_BACKOFF_MAX`=60 s).
- **Worker** (`PendingResolver`): reivindica linhas devidas com `FOR UPDATE SKIP LOCKED`
  e empurra `next_attempt_at` por um **lease** (`PENDING_LEASE`=30 s). Se a instância
  morre, o lease expira e outra retoma — inclusive após reinício. Cada linha é
  processada na própria transação (carteira → linha), com a mesma lógica do caminho síncrono.
- **Aceleração**: ao concluir uma transação com sucesso, `WakeDependents` torna devidas
  imediatamente as pendências que a referenciam (o backoff é o piso de garantia, não a latência típica).
- Uma operação pendente não movimenta dinheiro.

## 8. Reversões (`REFUND` e `ROLLBACK`)

| Operação | Referência permitida | Movimento |
| --- | --- | --- |
| `REFUND` | `BET` | crédito do valor da aposta |
| `ROLLBACK` | `BET`, `WIN` ou `REFUND` | contrário ao da referência (`BET`→crédito; `WIN`/`REFUND`→débito) |
| `WIN` com referência opcional | `BET` da mesma rodada | crédito (valor livre) |

Devem concordar provedor, jogador, carteira, moeda e rodada; o valor da reversão é
igual ao referenciado (sem reversão parcial). Falhas: `REFERENCE_MISMATCH`,
`REFERENCE_KIND_INVALID`, `REFERENCE_AMOUNT_MISMATCH`.

**Combinações `REFUND` × `ROLLBACK`**: uma transação referenciada recebe **no máximo uma
reversão bem-sucedida, de qualquer tipo** (índice único parcial
`wager_tx_one_reversal_per_reference` em `reference_transaction_id` para
`PROCESSED` + `REFUND|ROLLBACK`). Assim, `REFUND` seguido de `ROLLBACK` da mesma aposta
(ou o inverso) devolveria o mesmo débito duas vezes — o segundo é
`REJECTED / REFERENCE_ALREADY_REVERSED`. Reverter a própria reversão é coerente e
permitido em um único nível: `ROLLBACK` de um `REFUND` (débito) é aceito; `ROLLBACK`
de um `ROLLBACK` é `REFERENCE_KIND_INVALID`. A verificação ocorre sob o lock da carteira
(a corrida é serializada) e o índice único é a barreira final.

Reversão que exigiria debitar mais que o saldo: `REJECTED / REVERSAL_INSUFFICIENT_FUNDS`
(código diferente do `INSUFFICIENT_FUNDS` de uma `BET`), auditada, sem lançamento.

### Códigos de falha (`failureCode`)

Todo `failureCode` é **definitivo** para aquele `externalTransactionId` (o provedor deve
usar um novo id para tentar de novo). Entradas *corrigíveis* nunca chegam a ser
persistidas: são `400` (HTTP) ou DLQ (SQS).

| Código | Significado |
| --- | --- |
| `INSUFFICIENT_FUNDS` | `BET` sem saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | reversão que exigiria débito acima do saldo |
| `REFERENCE_NOT_FOUND` | referência não chegou dentro do orçamento |
| `REFERENCE_UNAVAILABLE` | referência existe mas continuou pendente até o fim do orçamento |
| `REFERENCE_NOT_PROCESSED` | referência terminou `REJECTED`/`FAILED` |
| `REFERENCE_MISMATCH` | provedor/jogador/carteira/moeda/rodada divergem |
| `REFERENCE_KIND_INVALID` | tipo da referência não aceito pela operação |
| `REFERENCE_AMOUNT_MISMATCH` | valor diferente do referenciado |
| `REFERENCE_ALREADY_REVERSED` | a referência já tem `REFUND`/`ROLLBACK` bem-sucedido |
| `WALLET_PLAYER_MISMATCH` | a carteira não pertence ao jogador informado |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da carteira |
| `PROCESSING_ERROR` | (`FAILED`) falha permanente registrada para auditoria |

## 9. Ledger e proteções do banco

`wallet_ledger_entries` é **append-only** e cada lançamento carrega
`id, wallet_id, transaction_id, direction, amount, currency, balance_before,
balance_after, wallet_version, created_at`.

No domínio, `NewLedgerEntry` valida `balanceAfter = balanceBefore ± money`. No banco
(`migrations/0001_init.up.sql`), independentemente do código:

- **Unicidade**: carteira por `(player_id, currency)`; `(provider_id, external_transaction_id)`;
  `(provider_id, idempotency_key)`; um `OPENING` por carteira; uma reversão por referência;
  lançamento por `(wallet_id, transaction_id)` e por `(wallet_id, wallet_version)`.
- **Não negatividade**: `balance_minor >= 0`, valores `> 0` nos lançamentos, `amount_minor`
  por tipo (`LOSS` = 0; demais > 0).
- **Forma**: transações internas (`OPENING`) e externas são mutuamente exclusivas em
  campos obrigatórios (CHECK `wager_tx_origin_shape`); coerência de estado × campos
  (`PROCESSED` exige saldo resultante; `REJECTED/FAILED`, código de falha).
- **Imutabilidade**: triggers recusam `UPDATE/DELETE/TRUNCATE` no ledger; transação
  terminal não muda e nenhuma transação/carteira é apagada; atributos de negócio da
  transação são imutáveis; o snapshot do outbox é imutável (só a contabilidade de entrega muda).
- **Consistência**: trigger de validação no `INSERT` do ledger (o lançamento pertence a uma
  transação `PROCESSED` da mesma carteira/moeda/valor; `LOSS` não tem lançamento; `BET`
  debita e `WIN/REFUND/OPENING` creditam; a cadeia de saldo/versão é contínua) e
  *constraint trigger* `DEFERRABLE INITIALLY DEFERRED` em `wallets`: no `COMMIT`, saldo e
  versão da carteira **têm de ser** os do último lançamento (ou `0`/`1` sem lançamentos).
  Não existe como o saldo mudar sem o lançamento correspondente.
- **Versão da carteira**: nasce em `1`; o crédito de abertura faz parte do estado inicial
  (lançamento de versão 1, `WalletBalanceChanged.walletVersion = 1`); uma carteira aberta
  com saldo 0 chega à versão 2 na primeira movimentação. Só há incremento quando o saldo muda.
  `LOSS` não cria lançamento nem altera versão.

Reconciliação (`POST /wallets/{id}/reconciliation`): snapshot `REPEATABLE READ, READ ONLY`
que lê o saldo armazenado e `SUM(crédito) − SUM(débito)` do ledger (incluindo a abertura),
`difference = armazenado − reconstruído`. Não altera nada; divergência vai para a
resposta, para o log (`ERROR`) e para `wagering_reconciliation_divergences_total`.

## 10. Inbox, consumidor SQS e DLQ

**Filas** (provisionadas por `wagering queues init`, idempotente):

| Fila | Papel |
| --- | --- |
| `wager-transactions.fifo` | entrada; `VisibilityTimeout` 60 s; *redrive* para a DLQ após `maxReceiveCount` = 5 |
| `wager-transactions-dlq.fifo` | mensagens inválidas / tentativas esgotadas (retenção 14 dias) |
| `wager-events.fifo` | saída dos eventos de integração |

**Produtor** (fora deste serviço): `MessageGroupId = walletId` (ordem por carteira, carteiras
em paralelo) e `MessageDeduplicationId = messageId` (a deduplicação do FIFO vale ~5 min e
**não** é confiável para a correção: a garantia financeira é a idempotência persistente).
O acesso é controlado por políticas de fila (produtores só enviam; o serviço consome/envia
à DLQ e aos eventos; consumidores de eventos só leem) e credenciais AWS; emuladores locais
não impõem IAM (ver §18).

**Fluxo por mensagem** (`messaging.Consumer`):

1. `ParseMessage` valida envelope e dados exatamente como o HTTP (mesmo construtor de
   domínio) — qualquer falha é **permanente**;
2. uma transação SQL: `inbox.Begin(consumidor, messageId, hash)` → `Processor.ProcessExternal`
   → `inbox.Complete`. Domínio, ledger, outbox e inbox confirmam **juntos**;
3. só depois do commit a mensagem é removida (`DeleteMessage`). Um crash entre o commit e a
   remoção apenas gera reentrega, que a inbox transforma em confirmação (sem novo efeito).

- **Hash da inbox** = `sha256(type \n idempotencyKey \n payloadHash)`; `payloadHash` é o mesmo
  do §6, então a mesma operação por HTTP e por SQS é reconhecida. `messageId` reutilizado com
  conteúdo diferente → DLQ (`message_id_conflict`).
- **Rejeição de negócio** é resultado confirmado: a mensagem é removida. Pendência de referência:
  a mensagem é concluída assim que a pendência está persistida; o worker assume a continuidade.
- **Falhas transitórias** (banco indisponível, lock timeout, cancelamento, carteira ainda
  inexistente): `ChangeMessageVisibility` com backoff exponencial (`CONSUMER_RETRY_BASE`=2 s …
  `CONSUMER_RETRY_MAX`=60 s). Ao atingir `SQS_MAX_RECEIVE_COUNT` (5), o consumidor envia a
  mensagem à DLQ (`retries_exhausted`); o *redrive* da fila é a rede de segurança.
- **Permanentes** (JSON inválido, tipo desconhecido, dinheiro inválido, `OPENING`, conflito
  de idempotência/`messageId`): DLQ imediata, com atributos `dlqReason`, `dlqError` e
  `originalMessageId`, e remoção da fila de entrada.
- Limites: `CONSUMER_PROCESS_TIMEOUT` (30 s) < `VisibilityTimeout` (60 s, validado na
  inicialização); lote de 5; 4 pollers por instância; long polling de 5 s.
- Em falha transitória o restante do lote é liberado (visibilidade 0) para preservar a
  ordem por carteira.
- **`SIGTERM`**: para de buscar mensagens; conclui a que está em andamento dentro do prazo ou
  cancela (a transação SQL sofre rollback) e libera a mensagem (visibilidade 0); o que sobrou
  do lote também é liberado (§14).

## 11. Outbox e publicação de eventos

- Eventos são gravados em `outbox_events` **na mesma transação** que estado, saldo, ledger e
  inbox. Nada é publicado antes do commit (o publisher só vê linhas confirmadas).
- **Publishers** (`messaging.Publisher`, quantos quiserem, em qualquer instância):
  `Claim` = `FOR UPDATE OF o SKIP LOCKED` + **lease** (`OUTBOX_LEASE`); cada claim conta uma
  tentativa; falha → `MarkFailed` com backoff (`OUTBOX_BACKOFF_BASE`=1 s … `MAX`=5 min);
  sucesso → `MarkPublished`. Uma linha abandonada (publisher morto) volta a ser elegível
  quando o lease expira.
- **Ordem por carteira**: só o evento **não publicado mais antigo de cada carteira** é
  elegível (`NOT EXISTS` de evento anterior não publicado). Publishers concorrentes nunca
  publicam um evento de uma carteira antes do anterior, mesmo que o anterior esteja aguardando
  retry.
- **At-least-once**: crash entre o `SendMessage` e o `MarkPublished` republica o mesmo evento
  com o **mesmo `eventId`** (payload é um snapshot imutável, serializado uma vez). Consumidores
  deduplicam por `eventId`; o FIFO usa `eventId` como `MessageDeduplicationId`.
- `FAULT_INJECTION` (somente testes) encerra o processo em pontos definidos:
  `consumer_after_commit` e `publisher_after_send`.

**Destino e roteamento**: fila `wager-events.fifo`; `MessageGroupId = walletId`,
`MessageDeduplicationId = eventId`; atributos `eventType` e `eventId` para filtro.

**Envelope**: `eventId, eventType, aggregateId, correlationId, causationId?, occurredAt (UTC, RFC 3339 com ms), version (=1), data`.

| Evento | Gatilho | `aggregateId` | `data` (além dos campos comuns) |
| --- | --- | --- | --- |
| `WagerTransactionProcessed` | conclusão com sucesso (inclui `LOSS` e `OPENING`) | transação | `transactionId, origin, kind, walletId, playerId, money, balance, referenceTransactionId?, processedAt` + `providerId, externalTransactionId, roundId, gameId, referenceExternalTransactionId` (ausentes no `OPENING`) |
| `WagerTransactionRejected` | rejeição definitiva | transação | idem + `failureCode, failureMessage, rejectedAt` |
| `WalletBalanceChanged` | mudança efetiva de saldo | carteira | `walletId, transactionId, ledgerEntryId, direction, money, balanceBefore, balanceAfter, walletVersion` |
| `WagerTransactionPendingReference` | início da espera por referência | transação | resumo + `expiresAt, nextAttemptAt` |

O tipo e a versão são definidos pelo **construtor do evento** no domínio. `LOSS` produz
`WagerTransactionProcessed` sem `WalletBalanceChanged`.

## 12. Contrato HTTP

| Situação | HTTP | Corpo |
| --- | --- | --- |
| Processada (ou replay) | `200` | `{transactionId, status:"PROCESSED", balance, idempotentReplay}` |
| Carteira criada | `201` | `{id, playerId, balance, version, createdAt, updatedAt}` + `Location` |
| Pendente de referência | `202` | `{transactionId, status:"PENDING_REFERENCE", idempotentReplay, expiresAt, nextAttemptAt}` + `Location: /wagering/transactions/{id}` |
| Entrada inválida | `400` | `{error:{code:"INVALID_REQUEST"\|"INVALID_MONEY"\|"INVALID_CURSOR", message, field, retryable:false, correlationId}}` — nada é persistido |
| Não autenticado | `401` | `UNAUTHENTICATED` + `WWW-Authenticate: Bearer` |
| Sem permissão / `providerId` diferente da identidade | `403` | `FORBIDDEN` / `PROVIDER_MISMATCH` |
| Não encontrado (ou de outro provedor, por id) | `404` | `WALLET_NOT_FOUND` / `TRANSACTION_NOT_FOUND` |
| Conflito | `409` | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| **Rejeição de negócio** | `422` | `{transactionId, status:"REJECTED", failureCode, failureMessage, balance, idempotentReplay}` |
| Tipo de conteúdo / tamanho | `415` / `413` | `UNSUPPORTED_MEDIA_TYPE` / `BODY_TOO_LARGE` (64 KiB) |
| Indisponibilidade transitória | `503` | `SERVICE_UNAVAILABLE`, `retryable:true`, `Retry-After: 1` — repetir com a mesma `Idempotency-Key` |
| Inesperado | `500` | `INTERNAL_ERROR` (sem detalhes internos) |

Endpoints: `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger?cursor&limit`,
`POST /wallets/{id}/reconciliation`, `POST /wagering/transactions`,
`GET /wagering/transactions/{id}`, `GET /providers/{providerId}/wagering/transactions/{externalId}`,
`GET /health/live`, `GET /health/ready`. Métricas em porta separada (`/metrics`, `METRICS_ADDR`).

- **Paginação do ledger**: cursor opaco (base64url de `{"b": walletVersion}`), ordem estável
  por `wallet_version` **decrescente** (mais recente primeiro; `(wallet_id, wallet_version)`
  é único e imutável); `limit` 1–200 (padrão 50); `nextCursor` é `null` na última página.
- Decodificação estrita: campos desconhecidos, dados após o JSON e números no lugar de
  strings de valor são `400`.
- `/health/ready` verifica PostgreSQL e SQS e **não** vaza detalhes do erro.

## 13. Autenticação e autorização

- **IdP**: Keycloak (OIDC), com `client_credentials` entre serviços. Escolhido por ser o
  recomendado, suportar importação declarativa de realm (provisionamento automático) e emitir
  JWT assinado (RS256) verificável offline via JWKS. O serviço **não** emite tokens nem guarda senhas.
- **Validação** (`go-oidc`): assinatura (JWKS carregado sob demanda e em cache — a API sobe
  antes do IdP e rejeita tudo até conseguir chaves), `iss`, `aud = wagering-api`, `exp`, `nbf`.
  Algoritmos aceitos: RS256/ES256/PS256. Toda rota de negócio exige token; nenhuma aceita
  credencial ausente, inválida ou expirada (`401`). O esquema deve ser `Bearer`.
- **Modelo de permissões** (claims do token validado):
  - papel de realm `wagering-provider` + claim `provider_id` (mapper fixo por cliente):
    **é a identidade que determina o `providerId` autorizado** — o corpo é comparado com ela
    antes de qualquer acesso ao banco;
  - papel `wagering-internal`: serviço interno de carteiras.

| Endpoint | provedor | interno |
| --- | :---: | :---: |
| `POST /wagering/transactions` | só como o próprio `providerId` | ✗ (`403`) |
| `GET /wagering/transactions/{id}` | só as suas (outras: `404`, sem revelar existência) | ✓ |
| `GET /providers/{p}/wagering/transactions/{e}` | só com `p` = a sua identidade (`403` caso contrário) | ✓ |
| `POST /wallets`, `GET /wallets/*`, `POST /wallets/*/reconciliation` | ✗ (`403`) | ✓ |
| `/health/*` | público | público |

  Replays herdam a mesma regra: outro provedor não consulta nem reaplica operações alheias
  (o namespace de idempotência é por provedor).
- **Mensageria**: controle por credenciais e políticas do broker; as validações de domínio
  continuam no consumidor (o corpo da mensagem não é confiável).
- **Identidades de teste** (realm `wagering`, `deploy/keycloak/wagering-realm.json`):
  `provider-a`, `provider-b`, `wallet-service`, `no-role-client` (sem papel → 403),
  `wrong-audience-client` (audiência errada → 401) e `expiring-provider` (token de 1 s, para
  testar expiração). Segredos: `<client>-secret`, apenas para uso local.

## 14. Uber Fx, ciclo de vida e shutdown

`internal/app` compõe tudo com `fx.Module` / `fx.Provide` / `fx.Invoke`, sempre por construtores:

| Módulo | Conteúdo |
| --- | --- |
| `observability` | logger JSON, métricas Prometheus, servidor de métricas |
| `postgres` | pool, `UnitOfWork`, leitores, filas (pendências/outbox), health check (grupo `health`) |
| `messaging` | cliente SQS, URLs das filas (resolvidas com retry), health check |
| `application` | `Processor`, serviços de carteira/aposta, `PendingResolver` |
| `http` | autenticador OIDC e servidor HTTP |
| `workers` | consumidor SQS, publisher, worker de pendências |

- **Início**: a configuração é validada antes de tudo (`config.Load`). No `OnStart` do pool o
  banco tem de responder **e** o schema estar migrado (senão a aplicação não sobe); as filas são
  resolvidas com retry; o HTTP é o **último** a iniciar (só recebe tráfego com tudo pronto).
- **Encerramento** (ordem inversa à do início, `fx.StopTimeout` = `SHUTDOWN_TIMEOUT`, 25 s):
  HTTP para de aceitar e espera as requisições em andamento → consumidor para de buscar,
  conclui ou libera a mensagem → publisher e worker de pendências terminam (o worker cancela o
  trabalho em curso; a transação sofre rollback) → **por último** o pool é fechado.
- O Fx aborta os `OnStop` restantes quando o contexto de parada expira; por isso cada
  componente recebe **metade do prazo restante** (`share`): um consumidor lento ou uma
  requisição presa não impedem os componentes seguintes nem o fechamento do pool.
- Workers têm `Running()` (término observável); `Stop` devolve erro se o prazo estourou.
- `fx.ValidateApp(app.Options(cfg))` valida o grafo para todas as 16 combinações de
  componentes habilitados, sem conectar a nada (teste unitário).

## 15. Observabilidade

- **Logs** JSON (`slog`) com `correlationId` (cabeçalho `X-Correlation-Id` ou gerado; no SQS =
  `messageId`), `messageId`, `transactionId`, `walletId`, `providerId`, `instanceId`. Nunca se
  registram tokens, cabeçalhos nem payloads financeiros; apenas identificadores, status e códigos.
- **Métricas** (`:9100/metrics`): `wagering_transactions_total{source,kind,status,failure_code}`,
  `wagering_duplicates_total{source,outcome=replay|conflict}`, `wagering_retries_total{component}`,
  `wagering_dlq_messages_total{reason}`, `wagering_concurrency_conflicts_total{reason=deadlock|serialization|lock_timeout|duplicate_insert}`,
  `wagering_wallet_lock_wait_seconds`, `wagering_processing_duration_seconds{source}`,
  `wagering_outbox_lag_seconds`, `wagering_outbox_published_total`, `wagering_outbox_publish_failures_total`,
  `wagering_reconciliation_divergences_total`, `wagering_sqs_messages_total{outcome}`, HTTP por rota,
  além das métricas de Go/processo.
- **Health**: `/health/live` (processo) e `/health/ready` (PostgreSQL + SQS).

## 16. Estratégia de testes

| Camada | Onde | O que cobre |
| --- | --- | --- |
| Unitários | `internal/**/_test.go` | `Money` (parsing, escala, limites, overflow, moedas, JSON, fuzz), carteira, ledger, máquina de estados, regras dos 5 tipos, hash canônico, abertura interna, casos de uso com repositório em memória (inclusive concorrência com `-race`), config, parsing de mensagens, autenticação com JWKS local, contrato HTTP |
| Integração (`-tags integration`) | `test/integration` | PostgreSQL, Keycloak e LocalStack **reais**; cada teste tem banco e filas próprios; instâncias do serviço são **processos do SO** independentes (conexões e memória próprias) |

Cenários exigidos e onde estão: 50 duplicatas em paralelo (3 processos), disputa 2×80 sobre 100,
carteiras distintas em paralelo (com prova de ausência de lock global), workload aleatório com
invariantes finais, HTTP×SQS concorrentes, crash pós-commit/antes do delete, publishers
concorrentes, crash entre publicar e confirmar, indisponibilidade do SQS, referência tardia e
expiração, pendências retomadas por outra instância, reinício, shutdown gracioso, schema
(constraints/triggers/migrations), autenticação real (expirado, audiência, papéis, isolamento).

## 17. Interpretações adotadas

- Uma **rejeição de negócio** é um resultado *persistido e definitivo* (`REJECTED`), respondido
  com `422`; `400`/DLQ é reservado a entradas inválidas (nada é persistido).
- Carteira inexistente: `404` no HTTP e, no SQS, falha **transitória** (a carteira pode ser
  criada logo depois); esgotada a cota de recebimentos, vai à DLQ.
- Jogador divergente da carteira e moeda diferente da carteira são rejeições persistidas
  (`WALLET_PLAYER_MISMATCH`, `CURRENCY_MISMATCH`).
- `WIN` com `referenceExternalTransactionId` valida a referência (`BET`, mesma rodada etc.) e
  espera por ela como as reversões; sem referência, é um crédito simples. `BET` e `LOSS` não
  aceitam referência.
- Uma reversão por referência, de qualquer tipo (§8).
- A abertura de carteira duplicada é sempre `409`, mesmo que idêntica.
- `expires_at` (TTL da espera) é avaliado com o relógio das instâncias; o agendamento dos
  retries (`next_attempt_at`) usa o relógio do banco (§3).
- Eventos de uma carteira saem em ordem (§11); entre carteiras não há ordem.

## 18. Limitações e trabalho não concluído

- **Testes de carga**, **tracing OpenTelemetry**, dashboards e **partidas dobradas** (todos
  opcionais no desafio) não foram implementados.
- **IAM/políticas de fila**: as políticas são provisionadas (`queues init`), mas LocalStack
  Community não as impõe; a validação de controle de acesso ao broker só é efetiva na AWS.
- JWT é sem estado: um token revogado continua válido até expirar (mitigação: tempo de vida curto
  no IdP). O endpoint de métricas não é autenticado (porta separada, para rede interna).
- Sem retenção/particionamento do ledger nem expurgo automático do outbox/inbox (o trigger permite
  apagar eventos já publicados; a rotina é operacional).
- Moedas limitadas às de 2 casas decimais; sem conversão entre moedas.
- Em ambientes onde o relógio do host de containers desvia ou salta (Docker Desktop após suspensão),
  o LocalStack pode atrasar reentregas FIFO e leases podem expirar antes; o sistema permanece
  correto (at-least-once + idempotência), mas testes que dependem de reentrega podem demorar ou
  precisar ser repetidos (ver README, *Solução de problemas*).
- O modo assíncrono de aceite (`PENDING` confirmado antes do processamento) não é usado pelo fluxo
  normal; sua retomada existe e é testada, mas apenas por inserção direta no banco.
