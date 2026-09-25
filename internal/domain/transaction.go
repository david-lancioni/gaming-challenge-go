package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// WagerTransaction registra uma operação sobre uma carteira: uma operação externa
// do provedor ou a abertura interna (OPENING). O estado é privado e só muda pelos
// métodos de transição:
//
//	PENDING ──► PROCESSED | REJECTED | FAILED
//	   │
//	   └──► PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED
//	            (RetryReference mantém o estado e agenda nova tentativa)
//
// PROCESSED, REJECTED e FAILED são terminais: nenhuma transição é aceita depois
// deles, e o banco impõe o mesmo por trigger. Um replay devolve o resultado gravado.
type WagerTransaction struct {
	id                             uuid.UUID
	origin                         Origin
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	payloadHash                    string
	walletID                       uuid.UUID
	playerID                       uuid.UUID
	roundID                        string
	gameID                         string
	kind                           TransactionKind
	money                          Money
	referenceExternalTransactionID string
	referenceTransactionID         *uuid.UUID
	status                         TransactionStatus
	failureCode                    FailureCode
	failureMessage                 string
	resultBalance                  *Money
	attempts                       int
	nextAttemptAt                  *time.Time
	expiresAt                      *time.Time
	correlationID                  string
	causationID                    string
	createdAt                      time.Time
	updatedAt                      time.Time
	processedAt                    *time.Time
	events                         []Event
}

type ExternalParams struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           TransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
	CorrelationID                  string
	CausationID                    string
}

func NewExternalTransaction(p ExternalParams, now time.Time) (*WagerTransaction, error) {
	if !p.Kind.IsExternal() {
		return nil, invalidWrap("kind", fmt.Errorf("%w: %q", ErrKindNotAllowed, string(p.Kind)))
	}
	if err := validateToken("providerId", p.ProviderID, MaxProviderIDLength); err != nil {
		return nil, err
	}
	if err := validateToken("externalTransactionId", p.ExternalTransactionID, MaxExternalIDLength); err != nil {
		return nil, err
	}
	if err := validateToken("idempotencyKey", p.IdempotencyKey, MaxIdempotencyKeyLength); err != nil {
		return nil, err
	}
	if err := validateToken("roundId", p.RoundID, MaxRoundIDLength); err != nil {
		return nil, err
	}
	if err := validateToken("gameId", p.GameID, MaxGameIDLength); err != nil {
		return nil, err
	}
	playerID, err := parseID("playerId", p.PlayerID)
	if err != nil {
		return nil, err
	}
	walletID, err := parseID("walletId", p.WalletID)
	if err != nil {
		return nil, err
	}
	if !p.Money.IsValid() {
		return nil, invalid("money", "is required")
	}
	if p.Money.IsNegative() {
		return nil, invalidWrap("money", ErrNegativeAmount)
	}
	if p.Kind == KindLoss {
		if !p.Money.IsZero() {
			return nil, invalid("money.amount", "must be 0.00 for LOSS")
		}
	} else if !p.Money.IsPositive() {
		return nil, invalid("money.amount", fmt.Sprintf("must be greater than zero for %s", p.Kind))
	}
	if p.ReferenceExternalTransactionID != "" {
		if err := validateToken("referenceExternalTransactionId", p.ReferenceExternalTransactionID, MaxExternalIDLength); err != nil {
			return nil, err
		}
		if p.ReferenceExternalTransactionID == p.ExternalTransactionID {
			return nil, invalid("referenceExternalTransactionId", "must differ from externalTransactionId")
		}
	}
	switch {
	case p.Kind.IsReversal() && p.ReferenceExternalTransactionID == "":
		return nil, invalid("referenceExternalTransactionId", fmt.Sprintf("is required for %s", p.Kind))
	case (p.Kind == KindBet || p.Kind == KindLoss) && p.ReferenceExternalTransactionID != "":
		return nil, invalid("referenceExternalTransactionId", fmt.Sprintf("is not allowed for %s", p.Kind))
	}
	if len(p.CorrelationID) > MaxCorrelationIDLength || len(p.CausationID) > MaxIdempotencyKeyLength {
		return nil, invalid("correlationId", "is too long")
	}
	now = now.UTC()
	return &WagerTransaction{
		id:                    NewID(),
		origin:                OriginExternal,
		providerID:            p.ProviderID,
		externalTransactionID: p.ExternalTransactionID,
		idempotencyKey:        p.IdempotencyKey,
		payloadHash: ComputePayloadHash(PayloadFields{
			ProviderID: p.ProviderID, ExternalTransactionID: p.ExternalTransactionID,
			PlayerID: playerID.String(), WalletID: walletID.String(), RoundID: p.RoundID, GameID: p.GameID,
			Kind: p.Kind, Money: p.Money, ReferenceExternalTransactionID: p.ReferenceExternalTransactionID,
		}),
		walletID: walletID, playerID: playerID, roundID: p.RoundID, gameID: p.GameID,
		kind: p.Kind, money: p.Money, referenceExternalTransactionID: p.ReferenceExternalTransactionID,
		status: StatusPending, nextAttemptAt: &now, correlationID: p.CorrelationID, causationID: p.CausationID,
		createdAt: now, updatedAt: now,
	}, nil
}

func newOpeningTransaction(w *Wallet, amount Money, correlationID string, now time.Time) (*WagerTransaction, error) {
	if !amount.IsPositive() {
		return nil, invalid("initialBalance", "OPENING requires a positive amount")
	}
	now = now.UTC()
	return &WagerTransaction{
		id: NewID(), origin: OriginInternal, walletID: w.id, playerID: w.playerID, kind: KindOpening,
		money: amount, status: StatusPending, nextAttemptAt: &now, correlationID: correlationID,
		createdAt: now, updatedAt: now,
	}, nil
}

type TransactionSnapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           TransactionKind
	Money                          Money
	ReferenceExternalTransactionID string
	ReferenceTransactionID         *uuid.UUID
	Status                         TransactionStatus
	FailureCode                    FailureCode
	FailureMessage                 string
	ResultBalance                  *Money
	Attempts                       int
	NextAttemptAt                  *time.Time
	ExpiresAt                      *time.Time
	CorrelationID                  string
	CausationID                    string
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ProcessedAt                    *time.Time
}

func RehydrateTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	bad := func(format string, a ...any) (*WagerTransaction, error) {
		return nil, fmt.Errorf("%w: transaction %s: %s", ErrInvalidState, s.ID, fmt.Sprintf(format, a...))
	}
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return bad("identifiers are required")
	}
	if !s.Status.isKnown() {
		return bad("unknown status %q", s.Status)
	}
	if !s.Money.IsValid() {
		return bad("money is uninitialized")
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.ProviderID != "" || s.ExternalTransactionID != "" || s.IdempotencyKey != "" {
			return bad("inconsistent internal origin")
		}
	case OriginExternal:
		if !s.Kind.IsExternal() || s.ProviderID == "" || s.ExternalTransactionID == "" || s.IdempotencyKey == "" || s.PayloadHash == "" {
			return bad("inconsistent external origin")
		}
	default:
		return bad("unknown origin %q", s.Origin)
	}
	switch s.Status {
	case StatusProcessed:
		if s.ResultBalance == nil {
			return bad("processed transaction without result balance")
		}
	case StatusRejected, StatusFailed:
		if s.FailureCode == "" {
			return bad("%s transaction without failure code", s.Status)
		}
	case StatusPendingReference:
		if s.ExpiresAt == nil {
			return bad("pending reference without expiration")
		}
	}
	return &WagerTransaction{
		id: s.ID, origin: s.Origin, providerID: s.ProviderID, externalTransactionID: s.ExternalTransactionID,
		idempotencyKey: s.IdempotencyKey, payloadHash: s.PayloadHash, walletID: s.WalletID, playerID: s.PlayerID,
		roundID: s.RoundID, gameID: s.GameID, kind: s.Kind, money: s.Money,
		referenceExternalTransactionID: s.ReferenceExternalTransactionID, referenceTransactionID: s.ReferenceTransactionID,
		status: s.Status, failureCode: s.FailureCode, failureMessage: s.FailureMessage, resultBalance: s.ResultBalance,
		attempts: s.Attempts, nextAttemptAt: s.NextAttemptAt, expiresAt: s.ExpiresAt,
		correlationID: s.CorrelationID, causationID: s.CausationID,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(), processedAt: s.ProcessedAt,
	}, nil
}

func (t *WagerTransaction) Snapshot() TransactionSnapshot {
	return TransactionSnapshot{
		ID: t.id, Origin: t.origin, ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
		IdempotencyKey: t.idempotencyKey, PayloadHash: t.payloadHash, WalletID: t.walletID, PlayerID: t.playerID,
		RoundID: t.roundID, GameID: t.gameID, Kind: t.kind, Money: t.money,
		ReferenceExternalTransactionID: t.referenceExternalTransactionID, ReferenceTransactionID: t.referenceTransactionID,
		Status: t.status, FailureCode: t.failureCode, FailureMessage: t.failureMessage, ResultBalance: t.resultBalance,
		Attempts: t.attempts, NextAttemptAt: t.nextAttemptAt, ExpiresAt: t.expiresAt,
		CorrelationID: t.correlationID, CausationID: t.causationID,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt, ProcessedAt: t.processedAt,
	}
}

func (t *WagerTransaction) ID() uuid.UUID                      { return t.id }
func (t *WagerTransaction) Origin() Origin                     { return t.origin }
func (t *WagerTransaction) ProviderID() string                 { return t.providerID }
func (t *WagerTransaction) ExternalTransactionID() string      { return t.externalTransactionID }
func (t *WagerTransaction) IdempotencyKey() string             { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string                { return t.payloadHash }
func (t *WagerTransaction) WalletID() uuid.UUID                { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID                { return t.playerID }
func (t *WagerTransaction) RoundID() string                    { return t.roundID }
func (t *WagerTransaction) GameID() string                     { return t.gameID }
func (t *WagerTransaction) Kind() TransactionKind              { return t.kind }
func (t *WagerTransaction) Money() Money                       { return t.money }
func (t *WagerTransaction) ReferenceExternalID() string        { return t.referenceExternalTransactionID }
func (t *WagerTransaction) ReferenceTransactionID() *uuid.UUID { return t.referenceTransactionID }
func (t *WagerTransaction) Status() TransactionStatus          { return t.status }
func (t *WagerTransaction) FailureCode() FailureCode           { return t.failureCode }
func (t *WagerTransaction) FailureMessage() string             { return t.failureMessage }
func (t *WagerTransaction) ResultBalance() *Money              { return t.resultBalance }
func (t *WagerTransaction) Attempts() int                      { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() *time.Time          { return t.nextAttemptAt }
func (t *WagerTransaction) ExpiresAt() *time.Time              { return t.expiresAt }
func (t *WagerTransaction) CorrelationID() string              { return t.correlationID }
func (t *WagerTransaction) CausationID() string                { return t.causationID }
func (t *WagerTransaction) CreatedAt() time.Time               { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time               { return t.updatedAt }
func (t *WagerTransaction) ProcessedAt() *time.Time            { return t.processedAt }

func (t *WagerTransaction) MovesMoney() bool { return t.kind != KindLoss }

func (t *WagerTransaction) transitionError(to TransactionStatus) error {
	return fmt.Errorf("%w: %s -> %s (transaction %s)", ErrInvalidTransition, t.status, to, t.id)
}

func (t *WagerTransaction) meta(now time.Time) EventMeta {
	causation := t.causationID
	if t.origin == OriginInternal {
		causation = t.id.String()
	}
	return EventMeta{CorrelationID: t.correlationID, CausationID: causation, OccurredAt: now}
}

func (t *WagerTransaction) summary() TransactionSummary {
	return TransactionSummary{
		TransactionID: t.id, Origin: t.origin, Kind: t.kind, WalletID: t.walletID, PlayerID: t.playerID,
		Money: t.money, ProviderID: t.providerID, ExternalTransactionID: t.externalTransactionID,
		RoundID: t.roundID, GameID: t.gameID, ReferenceExternalTransactionID: t.referenceExternalTransactionID,
	}
}

func (t *WagerTransaction) canTransition() bool {
	return t.status == StatusPending || t.status == StatusPendingReference
}

func (t *WagerTransaction) WaitForReference(now, nextAttemptAt, expiresAt time.Time) error {
	if t.status != StatusPending {
		return t.transitionError(StatusPendingReference)
	}
	if t.referenceExternalTransactionID == "" {
		return fmt.Errorf("%w: operation has no reference to wait for", ErrInvalidTransition)
	}
	now, next, exp := now.UTC(), nextAttemptAt.UTC(), expiresAt.UTC()
	t.status = StatusPendingReference
	t.nextAttemptAt, t.expiresAt = &next, &exp
	t.updatedAt = now
	t.events = append(t.events, NewWagerTransactionPendingReference(t.meta(now), WagerTransactionPendingReferenceData{
		TransactionSummary: t.summary(),
		ExpiresAt:          exp.Format(EventTimeLayout),
		NextAttemptAt:      next.Format(EventTimeLayout),
	}))
	return nil
}

func (t *WagerTransaction) RetryReference(now, nextAttemptAt time.Time) error {
	if t.status != StatusPendingReference {
		return t.transitionError(StatusPendingReference)
	}
	next := nextAttemptAt.UTC()
	t.attempts++
	t.nextAttemptAt = &next
	t.updatedAt = now.UTC()
	return nil
}

// MarkProcessed conclui a operação. balance é o saldo da carteira após a operação
// (inalterado no LOSS) e fica gravado para que um replay devolva o saldo observado
// no processamento original, mesmo que a carteira tenha mudado depois.
func (t *WagerTransaction) MarkProcessed(now time.Time, balance Money, referenceID *uuid.UUID) error {
	if !t.canTransition() {
		return t.transitionError(StatusProcessed)
	}
	if !balance.IsValid() || balance.IsNegative() || balance.Currency() != t.money.Currency() {
		return fmt.Errorf("%w: invalid result balance", ErrInvalidMoney)
	}
	now = now.UTC()
	t.status = StatusProcessed
	t.resultBalance = &balance
	t.referenceTransactionID = referenceID
	t.nextAttemptAt, t.processedAt, t.updatedAt = nil, &now, now
	t.events = append(t.events, NewWagerTransactionProcessed(t.meta(now), WagerTransactionProcessedData{
		TransactionSummary: t.summary(), Balance: balance, ReferenceTransactionID: referenceID,
		ProcessedAt: now.Format(EventTimeLayout),
	}))
	return nil
}

func (t *WagerTransaction) Reject(now time.Time, code FailureCode, message string, balance *Money) error {
	if !t.canTransition() {
		return t.transitionError(StatusRejected)
	}
	if code == "" {
		return invalid("failureCode", "is required")
	}
	now = now.UTC()
	t.status = StatusRejected
	t.failureCode, t.failureMessage, t.resultBalance = code, message, balance
	t.nextAttemptAt, t.processedAt, t.updatedAt = nil, &now, now
	t.events = append(t.events, NewWagerTransactionRejected(t.meta(now), WagerTransactionRejectedData{
		TransactionSummary: t.summary(), FailureCode: code, FailureMessage: message,
		RejectedAt: now.Format(EventTimeLayout),
	}))
	return nil
}

func (t *WagerTransaction) Fail(now time.Time, code FailureCode, message string) error {
	if !t.canTransition() {
		return t.transitionError(StatusFailed)
	}
	if code == "" {
		return invalid("failureCode", "is required")
	}
	now = now.UTC()
	t.status = StatusFailed
	t.failureCode, t.failureMessage = code, message
	t.nextAttemptAt, t.processedAt, t.updatedAt = nil, &now, now
	return nil
}

func (t *WagerTransaction) PullEvents() []Event {
	ev := t.events
	t.events = nil
	return ev
}
