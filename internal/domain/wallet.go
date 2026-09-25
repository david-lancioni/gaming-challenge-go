package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Wallet é a raiz do agregado financeiro; o par (playerId, currency) identifica uma
// única carteira. O saldo só muda por Debit e Credit, que produzem o lançamento do
// ledger correspondente e incrementam a versão em 1.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
	events    []Event
}

// NewWallet cria a carteira na versão 1. O saldo inicial é o estado da criação,
// não uma movimentação: o crédito de abertura é registrado por OpenWallet.
func NewWallet(playerID uuid.UUID, initial Money, now time.Time) (*Wallet, error) {
	if playerID == uuid.Nil {
		return nil, invalid("playerId", "must be a non-nil UUID")
	}
	if !initial.IsValid() {
		return nil, invalid("initialBalance", "is required")
	}
	if initial.IsNegative() {
		return nil, invalidWrap("initialBalance", ErrNegativeAmount)
	}
	now = now.UTC()
	return &Wallet{id: NewID(), playerID: playerID, balance: initial, version: 1, createdAt: now, updatedAt: now}, nil
}

// RehydrateWallet reconstrói uma carteira gravada. Não registra eventos nem aplica
// movimentações; apenas rejeita estados que violam as invariantes.
func RehydrateWallet(id, playerID uuid.UUID, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	switch {
	case id == uuid.Nil || playerID == uuid.Nil:
		return nil, fmt.Errorf("%w: identifiers are required", ErrInvalidWallet)
	case !balance.IsValid():
		return nil, fmt.Errorf("%w: balance is uninitialized", ErrInvalidWallet)
	case balance.IsNegative():
		return nil, fmt.Errorf("%w: negative balance %s", ErrInvalidWallet, balance)
	case version < 1:
		return nil, fmt.Errorf("%w: version must be >= 1", ErrInvalidWallet)
	}
	return &Wallet{id: id, playerID: playerID, balance: balance, version: version,
		createdAt: createdAt.UTC(), updatedAt: updatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Currency() Currency   { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

type Movement struct {
	TransactionID uuid.UUID
	Amount        Money
	Meta          EventMeta
}

func (w *Wallet) Debit(m Movement) (LedgerEntry, error) { return w.apply(DirectionDebit, m) }

func (w *Wallet) Credit(m Movement) (LedgerEntry, error) { return w.apply(DirectionCredit, m) }

func (w *Wallet) apply(dir Direction, m Movement) (LedgerEntry, error) {
	if m.TransactionID == uuid.Nil {
		return LedgerEntry{}, invalid("transactionId", "is required")
	}
	if !m.Amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: movement must be a positive amount", ErrInvalidMoney)
	}
	if m.Amount.Currency() != w.Currency() {
		return LedgerEntry{}, fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.Currency(), m.Amount.Currency())
	}
	var next Money
	var err error
	if dir == DirectionDebit {
		next, err = w.balance.Sub(m.Amount)
		if err == nil && next.IsNegative() {
			return LedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, w.balance, m.Amount)
		}
	} else {
		next, err = w.balance.Add(m.Amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	at := m.Meta.OccurredAt.UTC()
	entry, err := NewLedgerEntry(w.id, m.TransactionID, dir, m.Amount, w.balance, next, w.version+1, at)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.events = append(w.events, NewWalletBalanceChanged(m.Meta, WalletBalanceChangedData{
		WalletID: w.id, TransactionID: m.TransactionID, LedgerEntryID: entry.ID(), Direction: dir,
		Money: m.Amount, BalanceBefore: w.balance, BalanceAfter: next, WalletVersion: entry.WalletVersion(),
	}))
	w.balance = next
	w.version = entry.WalletVersion()
	w.updatedAt = at
	return entry, nil
}

func (w *Wallet) PullEvents() []Event {
	ev := w.events
	w.events = nil
	return ev
}

type WalletOpening struct {
	Wallet      *Wallet
	Transaction *WagerTransaction
	Entry       *LedgerEntry
	Events      []Event
}

// OpenWallet cria a carteira e, se o saldo inicial for positivo, a transação
// OPENING (já PROCESSED), o crédito de abertura e os eventos
// WagerTransactionProcessed e WalletBalanceChanged. Nesse caso a versão continua 1:
// o crédito de abertura faz parte do estado inicial. Saldo inicial zero não cria
// transação, lançamento nem evento.
func OpenWallet(playerID uuid.UUID, initial Money, correlationID string, now time.Time) (WalletOpening, error) {
	wallet, err := NewWallet(playerID, initial, now)
	if err != nil {
		return WalletOpening{}, err
	}
	if initial.IsZero() {
		return WalletOpening{Wallet: wallet}, nil
	}
	zero, err := Zero(initial.Currency())
	if err != nil {
		return WalletOpening{}, err
	}
	txn, err := newOpeningTransaction(wallet, initial, correlationID, now)
	if err != nil {
		return WalletOpening{}, err
	}
	entry, err := NewLedgerEntry(wallet.id, txn.id, DirectionCredit, initial, zero, initial, wallet.version, now)
	if err != nil {
		return WalletOpening{}, err
	}
	if err := txn.MarkProcessed(now, initial, nil); err != nil {
		return WalletOpening{}, err
	}
	meta := EventMeta{CorrelationID: correlationID, CausationID: txn.id.String(), OccurredAt: now}
	events := txn.PullEvents()
	events = append(events, NewWalletBalanceChanged(meta, WalletBalanceChangedData{
		WalletID: wallet.id, TransactionID: txn.id, LedgerEntryID: entry.ID(), Direction: DirectionCredit,
		Money: initial, BalanceBefore: zero, BalanceAfter: initial, WalletVersion: entry.WalletVersion(),
	}))
	return WalletOpening{Wallet: wallet, Transaction: txn, Entry: &entry, Events: events}, nil
}
