package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	money         Money
	balanceBefore Money
	balanceAfter  Money
	walletVersion int64
	createdAt     time.Time
}

func NewLedgerEntry(walletID, transactionID uuid.UUID, direction Direction, money, before, after Money, walletVersion int64, now time.Time) (LedgerEntry, error) {
	return buildLedgerEntry(NewID(), walletID, transactionID, direction, money, before, after, walletVersion, now)
}

func RehydrateLedgerEntry(id, walletID, transactionID uuid.UUID, direction Direction, money, before, after Money, walletVersion int64, createdAt time.Time) (LedgerEntry, error) {
	return buildLedgerEntry(id, walletID, transactionID, direction, money, before, after, walletVersion, createdAt)
}

func buildLedgerEntry(id, walletID, transactionID uuid.UUID, direction Direction, money, before, after Money, walletVersion int64, at time.Time) (LedgerEntry, error) {
	if id == uuid.Nil || walletID == uuid.Nil || transactionID == uuid.Nil {
		return LedgerEntry{}, fmt.Errorf("%w: identifiers are required", ErrInvalidLedger)
	}
	if walletVersion < 1 {
		return LedgerEntry{}, fmt.Errorf("%w: wallet version must be >= 1", ErrInvalidLedger)
	}
	if !money.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: movement must be a positive amount", ErrInvalidLedger)
	}
	if before.IsNegative() || after.IsNegative() || !before.IsValid() || !after.IsValid() {
		return LedgerEntry{}, fmt.Errorf("%w: balances must be initialized and non-negative", ErrInvalidLedger)
	}
	var expected Money
	var err error
	switch direction {
	case DirectionCredit:
		expected, err = before.Add(money)
	case DirectionDebit:
		expected, err = before.Sub(money)
	default:
		return LedgerEntry{}, fmt.Errorf("%w: unknown direction %q", ErrInvalidLedger, direction)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedger, err)
	}
	if !expected.Equal(after) {
		return LedgerEntry{}, fmt.Errorf("%w: balanceAfter %s != balanceBefore %s %s %s", ErrInvalidLedger, after, before, direction, money)
	}
	return LedgerEntry{id: id, walletID: walletID, transactionID: transactionID, direction: direction,
		money: money, balanceBefore: before, balanceAfter: after, walletVersion: walletVersion, createdAt: at.UTC()}, nil
}

func (e LedgerEntry) ID() uuid.UUID            { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID      { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }
func (e LedgerEntry) Direction() Direction     { return e.direction }
func (e LedgerEntry) Money() Money             { return e.money }
func (e LedgerEntry) BalanceBefore() Money     { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() Money      { return e.balanceAfter }

func (e LedgerEntry) WalletVersion() int64 { return e.walletVersion }
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }
