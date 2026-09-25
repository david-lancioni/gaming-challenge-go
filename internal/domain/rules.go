package domain

import "fmt"

type Rejection struct {
	Code    FailureCode
	Message string
}

func reject(code FailureCode, format string, a ...any) *Rejection {
	return &Rejection{Code: code, Message: fmt.Sprintf(format, a...)}
}

func (t *WagerTransaction) CheckWallet(w *Wallet) *Rejection {
	if w.ID() != t.walletID || w.PlayerID() != t.playerID {
		return reject(FailureWalletPlayerMismatch, "wallet does not belong to the informed player")
	}
	if w.Currency() != t.money.Currency() {
		return reject(FailureCurrencyMismatch, "operation currency %s differs from wallet currency %s", t.money.Currency(), w.Currency())
	}
	return nil
}

func (t *WagerTransaction) NeedsReference() bool { return t.referenceExternalTransactionID != "" }

func (t *WagerTransaction) CheckReference(ref *WagerTransaction) *Rejection {
	if ref.providerID != t.providerID || ref.walletID != t.walletID || ref.playerID != t.playerID ||
		ref.money.Currency() != t.money.Currency() || ref.roundID != t.roundID {
		return reject(FailureReferenceMismatch, "reference disagrees on provider, player, wallet, currency or round")
	}
	switch t.kind {
	case KindWin, KindRefund:
		if ref.kind != KindBet {
			return reject(FailureReferenceKindInvalid, "%s can only reference a BET, got %s", t.kind, ref.kind)
		}
	case KindRollback:
		if ref.kind != KindBet && ref.kind != KindWin && ref.kind != KindRefund {
			return reject(FailureReferenceKindInvalid, "ROLLBACK can only reference a BET, WIN or REFUND, got %s", ref.kind)
		}
	default:
		return reject(FailureReferenceKindInvalid, "%s does not accept a reference", t.kind)
	}
	if t.kind.IsReversal() && !t.money.Equal(ref.money) {
		return reject(FailureReferenceAmountMismatch, "reversal amount %s differs from referenced amount %s", t.money, ref.money)
	}
	return nil
}

func (t *WagerTransaction) MovementDirection(ref *WagerTransaction) (dir Direction, ok bool) {
	switch t.kind {
	case KindBet:
		return DirectionDebit, true
	case KindWin, KindRefund:
		return DirectionCredit, true
	case KindRollback:
		if ref == nil {
			return "", false
		}
		if ref.kind == KindBet {
			return DirectionCredit, true
		}
		return DirectionDebit, true
	}
	return "", false
}

func (t *WagerTransaction) InsufficientFundsCode() FailureCode {
	if t.kind == KindBet {
		return FailureInsufficientFunds
	}
	return FailureReversalInsufficientFunds
}
