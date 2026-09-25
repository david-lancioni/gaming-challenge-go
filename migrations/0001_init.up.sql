-- Invariantes financeiras impostas pelo próprio banco, independentemente dos locks
-- da aplicação e da deduplicação do SQS FIFO:
--   * unicidade: carteira por (jogador, moeda); (provedor, id externo);
--     (provedor, chave de idempotência); uma abertura por carteira; uma reversão
--     bem-sucedida por transação referenciada; um lançamento por (carteira,
--     transação) e por (carteira, versão).
--   * não negatividade: saldos e valores com CHECK.
--   * imutabilidade: ledger append-only, transações terminais não mudam,
--     carteiras não são apagadas (triggers).
--   * consistência: um trigger adiado garante, no commit, que saldo e versão da
--     carteira são iguais aos do último lançamento do ledger.

CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CONSTRAINT wallets_version_positive CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('EXTERNAL', 'INTERNAL')),
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    wallet_id                         uuid        NOT NULL REFERENCES wallets (id),
    player_id                         uuid        NOT NULL,
    round_id                          text,
    game_id                           text,
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    amount_minor                      bigint      NOT NULL CHECK (amount_minor >= 0),
    currency                          char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code                      text,
    failure_message                   text,
    result_balance_minor              bigint      CHECK (result_balance_minor IS NULL OR result_balance_minor >= 0),
    attempts                          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    expires_at                        timestamptz,
    correlation_id                    text,
    causation_id                      text,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    processed_at                      timestamptz,

    -- Operações internas (OPENING) e externas têm formatos distintos e exclusivos.
    CONSTRAINT wager_tx_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT wager_tx_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_tx_reversal_reference CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_tx_status_shape CHECK (
        (status = 'PROCESSED' AND result_balance_minor IS NOT NULL AND failure_code IS NULL)
        OR (status IN ('REJECTED', 'FAILED') AND failure_code IS NOT NULL)
        OR (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL)
    ),
    CONSTRAINT wager_tx_pending_reference_ttl CHECK (status <> 'PENDING_REFERENCE' OR expires_at IS NOT NULL),
    CONSTRAINT wager_tx_resolved_reference CHECK (status = 'PROCESSED' OR reference_transaction_id IS NULL),

    -- Idempotência persistente: sobrevive ao reinício de todos os processos.
    CONSTRAINT wager_tx_provider_external_key UNIQUE (provider_id, external_transaction_id),
    CONSTRAINT wager_tx_provider_idempotency_key UNIQUE (provider_id, idempotency_key)
);

-- No máximo um crédito de abertura por carteira.
CREATE UNIQUE INDEX wager_tx_one_opening_per_wallet ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

-- Uma transação referenciada recebe no máximo uma reversão bem-sucedida, de
-- qualquer tipo: REFUND e ROLLBACK da mesma BET devolveriam o mesmo débito duas vezes.
CREATE UNIQUE INDEX wager_tx_one_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX wager_tx_due_idx ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');
CREATE INDEX wager_tx_waiting_for_idx ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_tx_wallet_idx ON wager_transactions (wallet_id, created_at);

CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    wallet_id            uuid        NOT NULL REFERENCES wallets (id),
    transaction_id       uuid        NOT NULL REFERENCES wager_transactions (id),
    direction            text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         bigint      NOT NULL CHECK (amount_minor > 0),
    currency             char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor bigint      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  bigint      NOT NULL CHECK (balance_after_minor >= 0),
    wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
    created_at           timestamptz NOT NULL,
    CONSTRAINT ledger_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT ledger_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT ledger_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,
    seq             bigint      GENERATED ALWAYS AS IDENTITY,
    aggregate_type  text        NOT NULL,
    aggregate_id    uuid        NOT NULL,
    partition_key   uuid        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_by       text,
    locked_until    timestamptz,
    last_error      text,
    published_at    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now()
);
-- O claim do publisher percorre os eventos pendentes por seq e só pega o mais
-- antigo de cada carteira (partition_key), preservando a ordem por carteira.
CREATE INDEX outbox_unpublished_seq_idx ON outbox_events (seq) WHERE published_at IS NULL;
CREATE INDEX outbox_partition_seq_idx ON outbox_events (partition_key, seq) WHERE published_at IS NULL;

CREATE TABLE inbox_messages (
    consumer_name text        NOT NULL,
    message_id    text        NOT NULL,
    payload_hash  text        NOT NULL,
    received_at   timestamptz NOT NULL,
    completed_at  timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

-- Ledger append-only: UPDATE, DELETE e TRUNCATE são recusados.
CREATE FUNCTION ledger_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% not allowed)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END $$;

CREATE TRIGGER ledger_no_update_delete BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_append_only();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_append_only();

REVOKE UPDATE, DELETE, TRUNCATE ON wallet_ledger_entries FROM PUBLIC;

-- Cada lançamento precisa corresponder à sua transação PROCESSED (carteira, valor,
-- moeda, direção coerente com o tipo) e continuar a cadeia da carteira:
-- versão = anterior + 1 e saldo_antes = saldo_depois do lançamento anterior.
CREATE FUNCTION ledger_validate_entry() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t   wager_transactions%ROWTYPE;
    w   wallets%ROWTYPE;
    last_version bigint;
    last_after   bigint;
BEGIN
    SELECT * INTO t FROM wager_transactions WHERE id = NEW.transaction_id;
    IF t.wallet_id <> NEW.wallet_id OR t.status <> 'PROCESSED'
       OR t.amount_minor <> NEW.amount_minor OR t.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger entry does not match its processed transaction %', NEW.transaction_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF t.kind = 'LOSS' THEN
        RAISE EXCEPTION 'LOSS transactions must not produce ledger entries'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF (t.kind = 'BET' AND NEW.direction <> 'DEBIT')
       OR (t.kind IN ('WIN', 'REFUND', 'OPENING') AND NEW.direction <> 'CREDIT') THEN
        RAISE EXCEPTION '% must not produce a % entry', t.kind, NEW.direction
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    SELECT * INTO w FROM wallets WHERE id = NEW.wallet_id;
    IF w.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger currency % differs from wallet currency %', NEW.currency, w.currency
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    SELECT wallet_version, balance_after_minor INTO last_version, last_after
      FROM wallet_ledger_entries WHERE wallet_id = NEW.wallet_id
     ORDER BY wallet_version DESC LIMIT 1;
    IF NOT FOUND THEN
        -- Primeiro lançamento: começa do saldo 0. O crédito de abertura faz parte do
        -- estado inicial (versão 1); uma carteira aberta vazia chega à versão 2 no
        -- primeiro movimento.
        IF NEW.balance_before_minor <> 0
           OR NEW.wallet_version <> (CASE WHEN t.kind = 'OPENING' THEN 1 ELSE 2 END) THEN
            RAISE EXCEPTION 'first ledger entry of wallet % must start at balance 0 and version % (opening) or 2', NEW.wallet_id, 1
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF t.kind = 'OPENING' THEN
        RAISE EXCEPTION 'OPENING must be the first ledger entry of wallet %', NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
    ELSIF NEW.wallet_version <> last_version + 1 OR NEW.balance_before_minor <> last_after THEN
        RAISE EXCEPTION 'ledger entry of wallet % breaks the balance chain', NEW.wallet_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER ledger_validate BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_validate_entry();

-- Carteiras: identidade imutável, saldo e versão mudam juntos (+1) e não há DELETE.
CREATE FUNCTION wallets_guard_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor THEN
        IF NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'wallet % balance changed without a version increment by one', OLD.id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet % version changed without a balance change', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER wallets_guard BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();

-- Constraint trigger adiado (roda no commit): saldo e versão da carteira precisam
-- ser iguais aos do último lançamento (ou 0 / 1 sem lançamentos). O saldo nunca
-- muda sem o lançamento correspondente no ledger.
CREATE FUNCTION wallets_enforce_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    last_version bigint;
    last_after   bigint;
BEGIN
    SELECT wallet_version, balance_after_minor INTO last_version, last_after
      FROM wallet_ledger_entries WHERE wallet_id = NEW.id
     ORDER BY wallet_version DESC LIMIT 1;
    IF NOT FOUND THEN
        IF NEW.balance_minor <> 0 OR NEW.version <> 1 THEN
            RAISE EXCEPTION 'wallet % has no ledger entries but balance=% version=%',
                NEW.id, NEW.balance_minor, NEW.version
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
    ELSIF NEW.version <> last_version OR NEW.balance_minor <> last_after THEN
        RAISE EXCEPTION 'wallet % (balance=%, version=%) disagrees with its ledger (balance=%, version=%)',
            NEW.id, NEW.balance_minor, NEW.version, last_after, last_version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER wallets_ledger_consistency AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_enforce_ledger_consistency();

-- Transações: atributos de negócio imutáveis, estados terminais são finais,
-- PENDING_REFERENCE nunca volta para PENDING e não há DELETE.
CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager_transactions cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is in terminal state % and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
        RAISE EXCEPTION 'transaction % cannot return to PENDING', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.player_id <> OLD.player_id OR NEW.kind <> OLD.kind OR NEW.amount_minor <> OLD.amount_minor
       OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.round_id IS DISTINCT FROM OLD.round_id
       OR NEW.game_id IS DISTINCT FROM OLD.game_id
       OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id THEN
        RAISE EXCEPTION 'transaction % business attributes are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER wager_transactions_guard BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

-- Outbox: o snapshot do evento é imutável; só os campos de controle de entrega
-- mudam. Eventos só podem ser apagados depois de publicados.
CREATE FUNCTION outbox_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NULL THEN
            RAISE EXCEPTION 'unpublished outbox event % cannot be deleted', OLD.id
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.id <> OLD.id OR NEW.seq <> OLD.seq OR NEW.aggregate_type <> OLD.aggregate_type
       OR NEW.aggregate_id <> OLD.aggregate_id OR NEW.partition_key <> OLD.partition_key
       OR NEW.event_type <> OLD.event_type OR NEW.event_version <> OLD.event_version
       OR NEW.payload <> OLD.payload OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER outbox_guard BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard();
