package domain

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

func NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

type TransactionKind string

const (
	KindOpening  TransactionKind = "OPENING"
	KindBet      TransactionKind = "BET"
	KindWin      TransactionKind = "WIN"
	KindLoss     TransactionKind = "LOSS"
	KindRefund   TransactionKind = "REFUND"
	KindRollback TransactionKind = "ROLLBACK"
)

func (k TransactionKind) IsExternal() bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

func (k TransactionKind) IsReversal() bool { return k == KindRefund || k == KindRollback }

type TransactionStatus string

const (
	StatusPending          TransactionStatus = "PENDING"
	StatusPendingReference TransactionStatus = "PENDING_REFERENCE"
	StatusProcessed        TransactionStatus = "PROCESSED"
	StatusRejected         TransactionStatus = "REJECTED"
	StatusFailed           TransactionStatus = "FAILED"
)

func (s TransactionStatus) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

func (s TransactionStatus) isKnown() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

type Origin string

const (
	OriginExternal Origin = "EXTERNAL"
	OriginInternal Origin = "INTERNAL"
)

func parseID(field, s string) (uuid.UUID, error) {
	if len(s) != 36 {
		return uuid.Nil, invalid(field, "must be a canonical UUID")
	}
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, invalid(field, "must be a non-nil UUID")
	}
	return id, nil
}

func ParseID(field, s string) (uuid.UUID, error) { return parseID(field, s) }

func validateToken(field, s string, max int) error {
	if s == "" {
		return invalid(field, "is required")
	}
	if !utf8.ValidString(s) {
		return invalid(field, "must be valid UTF-8")
	}
	if utf8.RuneCountInString(s) > max {
		return invalid(field, fmt.Sprintf("must have at most %d characters", max))
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return invalid(field, "must not contain control characters")
		}
	}
	if r, _ := utf8.DecodeRuneInString(s); unicode.IsSpace(r) {
		return invalid(field, "must not start or end with whitespace")
	}
	if r, _ := utf8.DecodeLastRuneInString(s); unicode.IsSpace(r) {
		return invalid(field, "must not start or end with whitespace")
	}
	return nil
}

func ValidateToken(field, s string, max int) error { return validateToken(field, s, max) }

const (
	MaxProviderIDLength     = 128
	MaxExternalIDLength     = 128
	MaxRoundIDLength        = 128
	MaxGameIDLength         = 128
	MaxIdempotencyKeyLength = 255
	MaxCorrelationIDLength  = 128
)
