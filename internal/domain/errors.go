package domain

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidMoney        = errors.New("invalid money")
	ErrNegativeAmount      = errors.New("negative amount")
	ErrScaleExceeded       = errors.New("amount scale exceeded")
	ErrMoneyOverflow       = errors.New("money overflow")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrUnsupportedCurrency = errors.New("unsupported currency")

	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidWallet     = errors.New("invalid wallet")
	ErrInvalidLedger     = errors.New("invalid ledger entry")

	ErrInvalidInput      = errors.New("invalid input")
	ErrKindNotAllowed    = errors.New("transaction kind not allowed on this entry point")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrInvalidState      = errors.New("invalid persisted state")

	ErrIdempotencyConflict         = errors.New("idempotency key reused with a different payload")
	ErrExternalTransactionConflict = errors.New("external transaction already registered under another idempotency key")

	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	ErrTransactionNotFound = errors.New("transaction not found")
)

type ValidationError struct {
	Field  string
	Reason string
	Err    error
}

func (e *ValidationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("invalid %s: %s: %v", e.Field, e.Reason, e.Err)
	}
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

func (e *ValidationError) Is(target error) bool { return target == ErrInvalidInput }
func (e *ValidationError) Unwrap() error        { return e.Err }

func NewValidationError(field, reason string) error { return invalid(field, reason) }

func NewValidationErrorWrap(field string, cause error) error { return invalidWrap(field, cause) }

func invalid(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}

func invalidWrap(field string, err error) error {
	return &ValidationError{Field: field, Reason: err.Error(), Err: err}
}

type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceUnavailable      FailureCode = "REFERENCE_UNAVAILABLE"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceKindInvalid      FailureCode = "REFERENCE_KIND_INVALID"
	FailureReferenceAmountMismatch   FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureWalletPlayerMismatch      FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailureProcessingError           FailureCode = "PROCESSING_ERROR"
)
