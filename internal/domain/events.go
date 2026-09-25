package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventWagerTransactionProcessed        EventType = "WagerTransactionProcessed"
	EventWagerTransactionRejected         EventType = "WagerTransactionRejected"
	EventWalletBalanceChanged             EventType = "WalletBalanceChanged"
	EventWagerTransactionPendingReference EventType = "WagerTransactionPendingReference"
)

const EventVersion = 1

type EventData interface {
	eventType() EventType
	aggregate() (aggregateType string, aggregateID uuid.UUID)
}

type EventMeta struct {
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

type Event struct {
	id            uuid.UUID
	eventType     EventType
	version       int
	aggregateType string
	aggregateID   uuid.UUID
	partitionKey  uuid.UUID
	correlationID string
	causationID   string
	occurredAt    time.Time
	data          EventData
}

func newEvent(meta EventMeta, walletID uuid.UUID, data EventData) Event {
	aggType, aggID := data.aggregate()
	return Event{
		id:            NewID(),
		eventType:     data.eventType(),
		version:       EventVersion,
		aggregateType: aggType,
		aggregateID:   aggID,
		partitionKey:  walletID,
		correlationID: meta.CorrelationID,
		causationID:   meta.CausationID,
		occurredAt:    meta.OccurredAt.UTC(),
		data:          data,
	}
}

func (e Event) ID() uuid.UUID          { return e.id }
func (e Event) Type() EventType        { return e.eventType }
func (e Event) Version() int           { return e.version }
func (e Event) AggregateType() string  { return e.aggregateType }
func (e Event) AggregateID() uuid.UUID { return e.aggregateID }
func (e Event) OccurredAt() time.Time  { return e.occurredAt }
func (e Event) CorrelationID() string  { return e.correlationID }
func (e Event) CausationID() string    { return e.causationID }
func (e Event) Data() EventData        { return e.data }

func (e Event) PartitionKey() uuid.UUID { return e.partitionKey }

const EventTimeLayout = "2006-01-02T15:04:05.000Z"

type envelopeJSON struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     EventType `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    string    `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          EventData `json:"data"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(envelopeJSON{
		EventID:       e.id,
		EventType:     e.eventType,
		AggregateID:   e.aggregateID,
		CorrelationID: e.correlationID,
		CausationID:   e.causationID,
		OccurredAt:    e.occurredAt.UTC().Format(EventTimeLayout),
		Version:       e.version,
		Data:          e.data,
	})
}

func RehydrateEvent(id uuid.UUID, t EventType, version int, aggType string, aggID, partition uuid.UUID, correlationID, causationID string, occurredAt time.Time, data EventData) Event {
	return Event{id: id, eventType: t, version: version, aggregateType: aggType, aggregateID: aggID,
		partitionKey: partition, correlationID: correlationID, causationID: causationID,
		occurredAt: occurredAt.UTC(), data: data}
}

type TransactionSummary struct {
	TransactionID                  uuid.UUID       `json:"transactionId"`
	Origin                         Origin          `json:"origin"`
	Kind                           TransactionKind `json:"kind"`
	WalletID                       uuid.UUID       `json:"walletId"`
	PlayerID                       uuid.UUID       `json:"playerId"`
	Money                          Money           `json:"money"`
	ProviderID                     string          `json:"providerId,omitempty"`
	ExternalTransactionID          string          `json:"externalTransactionId,omitempty"`
	RoundID                        string          `json:"roundId,omitempty"`
	GameID                         string          `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId,omitempty"`
}

type WagerTransactionProcessedData struct {
	TransactionSummary
	Balance                Money      `json:"balance"`
	ReferenceTransactionID *uuid.UUID `json:"referenceTransactionId,omitempty"`
	ProcessedAt            string     `json:"processedAt"`
}

func (d WagerTransactionProcessedData) eventType() EventType { return EventWagerTransactionProcessed }
func (d WagerTransactionProcessedData) aggregate() (string, uuid.UUID) {
	return "WagerTransaction", d.TransactionID
}

type WagerTransactionRejectedData struct {
	TransactionSummary
	FailureCode    FailureCode `json:"failureCode"`
	FailureMessage string      `json:"failureMessage,omitempty"`
	RejectedAt     string      `json:"rejectedAt"`
}

func (d WagerTransactionRejectedData) eventType() EventType { return EventWagerTransactionRejected }
func (d WagerTransactionRejectedData) aggregate() (string, uuid.UUID) {
	return "WagerTransaction", d.TransactionID
}

type WagerTransactionPendingReferenceData struct {
	TransactionSummary
	ExpiresAt     string `json:"expiresAt"`
	NextAttemptAt string `json:"nextAttemptAt"`
}

func (d WagerTransactionPendingReferenceData) eventType() EventType {
	return EventWagerTransactionPendingReference
}
func (d WagerTransactionPendingReferenceData) aggregate() (string, uuid.UUID) {
	return "WagerTransaction", d.TransactionID
}

type WalletBalanceChangedData struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	LedgerEntryID uuid.UUID `json:"ledgerEntryId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

func (d WalletBalanceChangedData) eventType() EventType { return EventWalletBalanceChanged }
func (d WalletBalanceChangedData) aggregate() (string, uuid.UUID) {
	return "Wallet", d.WalletID
}

func NewWagerTransactionProcessed(meta EventMeta, data WagerTransactionProcessedData) Event {
	return newEvent(meta, data.WalletID, data)
}

func NewWagerTransactionRejected(meta EventMeta, data WagerTransactionRejectedData) Event {
	return newEvent(meta, data.WalletID, data)
}

func NewWagerTransactionPendingReference(meta EventMeta, data WagerTransactionPendingReferenceData) Event {
	return newEvent(meta, data.WalletID, data)
}

func NewWalletBalanceChanged(meta EventMeta, data WalletBalanceChangedData) Event {
	return newEvent(meta, data.WalletID, data)
}
