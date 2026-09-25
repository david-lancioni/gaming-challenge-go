package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

type PendingPolicy struct {
	TTL         time.Duration
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

type Outcome struct {
	Transaction *domain.WagerTransaction
	Replay      bool
}

// Processor é a única implementação das regras financeiras: HTTP, SQS e o worker
// de pendências o chamam dentro de uma transação SQL aberta por eles, então todas
// as entradas têm as mesmas garantias.
//
// Ordem dos locks: toda alteração de uma carteira ou das transações dela acontece
// com o lock da linha da carteira (SELECT ... FOR UPDATE), sempre adquirido
// primeiro. Com uma ordem única não há deadlock entre operações da mesma carteira,
// e carteiras diferentes nunca disputam o mesmo lock (não existe lock global).
type Processor struct {
	clock   Clock
	policy  PendingPolicy
	metrics Metrics
}

func NewProcessor(clock Clock, policy PendingPolicy, metrics Metrics) *Processor {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &Processor{clock: clock, policy: policy, metrics: metrics}
}

// ProcessExternal aplica uma operação do provedor na transação SQL do chamador.
// Os agregados são reconstruídos a cada chamada porque a UnitOfWork pode reexecutar
// a função após deadlock ou falha de serialização.
//
// Rejeições de negócio não são erros: ficam persistidas como REJECTED e voltam num
// Outcome normal. Erros ficam para entrada inválida, conflito de idempotência e
// falhas de infraestrutura.
func (p *Processor) ProcessExternal(ctx context.Context, r Repositories, params domain.ExternalParams) (Outcome, error) {
	now := p.clock()
	txn, err := domain.NewExternalTransaction(params, now)
	if err != nil {
		return Outcome{}, err
	}

	wallet, err := p.lockWallet(ctx, r, txn.WalletID())
	if err != nil {
		return Outcome{}, err
	}

	// Verificação de idempotência definitiva: feita já com o lock da carteira, então
	// duas cópias da mesma operação não passam juntas por aqui. A consulta sem lock
	// do WageringService é só um atalho para replays.
	existing, err := r.Transactions().FindByProviderKeys(ctx, txn.ProviderID(), txn.IdempotencyKey(), txn.ExternalTransactionID())
	if err != nil {
		return Outcome{}, err
	}
	if len(existing) > 0 {
		prior, err := ClassifyExisting(txn, existing)
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{Transaction: prior, Replay: true}, nil
	}

	// Versão lida sob lock; o UPDATE do saldo exige que ela não tenha mudado.
	expected := wallet.Version()
	entry, err := p.resolve(ctx, r, wallet, txn, now)
	if err != nil {
		return Outcome{}, err
	}
	if err := p.persist(ctx, r, wallet, txn, entry, expected, true); err != nil {
		return Outcome{}, err
	}
	return Outcome{Transaction: txn}, nil
}

// ClassifyExisting decide o que significa uma operação repetida, dadas as
// transações já gravadas com a mesma chave de idempotência ou o mesmo id externo:
//
//   - mesma chave e mesmo hash de payload: replay, devolve o resultado original;
//   - mesma chave e payload diferente: ErrIdempotencyConflict;
//   - mesmo id externo com outra chave: ErrExternalTransactionConflict, para que
//     uma operação financeira nunca seja reaplicada trocando a chave.
func ClassifyExisting(incoming *domain.WagerTransaction, existing []*domain.WagerTransaction) (*domain.WagerTransaction, error) {
	for _, e := range existing {
		if e.IdempotencyKey() == incoming.IdempotencyKey() {
			if e.PayloadHash() != incoming.PayloadHash() {
				return nil, domain.ErrIdempotencyConflict
			}
			return e, nil
		}
	}
	if len(existing) > 0 {
		return nil, domain.ErrExternalTransactionConflict
	}
	return nil, nil
}

func (p *Processor) lockWallet(ctx context.Context, r Repositories, id uuid.UUID) (*domain.Wallet, error) {
	start := time.Now()
	w, err := r.Wallets().GetForUpdate(ctx, id)
	p.metrics.WalletLockWait(time.Since(start))
	return w, err
}

// ResolvePending retoma uma operação PENDING ou PENDING_REFERENCE reivindicada pelo
// worker. Mesma ordem de locks do fluxo principal: carteira primeiro, depois a
// transação. Devolve nil se outra instância já concluiu a operação.
func (p *Processor) ResolvePending(ctx context.Context, r Repositories, due DueTransaction) (*domain.WagerTransaction, error) {
	wallet, err := p.lockWallet(ctx, r, due.WalletID)
	if err != nil {
		return nil, err
	}
	txn, err := r.Transactions().GetByIDForUpdate(ctx, due.ID)
	if err != nil {
		return nil, err
	}
	if txn.Status().IsTerminal() {
		return nil, nil
	}
	now := p.clock()
	expected := wallet.Version()
	entry, err := p.resolve(ctx, r, wallet, txn, now)
	if err != nil {
		return nil, err
	}
	if err := p.persist(ctx, r, wallet, txn, entry, expected, false); err != nil {
		return nil, err
	}
	return txn, nil
}

// FailPending registra uma falha permanente (FAILED), para auditoria, quando uma
// pendência não pode ser processada por nenhuma nova tentativa.
func (p *Processor) FailPending(ctx context.Context, r Repositories, due DueTransaction, cause error) (*domain.WagerTransaction, error) {
	if _, err := p.lockWallet(ctx, r, due.WalletID); err != nil {
		return nil, err
	}
	txn, err := r.Transactions().GetByIDForUpdate(ctx, due.ID)
	if err != nil {
		return nil, err
	}
	if txn.Status().IsTerminal() {
		return nil, nil
	}
	msg := cause.Error()
	if len(msg) > 500 {
		msg = msg[:500]
	}
	if err := txn.Fail(p.clock(), domain.FailureProcessingError, msg); err != nil {
		return nil, err
	}
	if err := r.Transactions().Update(ctx, txn); err != nil {
		return nil, err
	}
	return txn, nil
}

// resolve decide o resultado de txn e aplica o efeito financeiro na carteira
// (travada), em memória. Ao retornar, txn está PROCESSED, REJECTED ou aguardando
// a referência; o lançamento do ledger, se houver, é devolvido para persistência.
func (p *Processor) resolve(ctx context.Context, r Repositories, wallet *domain.Wallet, txn *domain.WagerTransaction, now time.Time) (*domain.LedgerEntry, error) {
	reject := func(rej *domain.Rejection) (*domain.LedgerEntry, error) {
		balance := wallet.Balance()
		return nil, txn.Reject(now, rej.Code, rej.Message, &balance)
	}

	if rej := txn.CheckWallet(wallet); rej != nil {
		return reject(rej)
	}

	var ref *domain.WagerTransaction
	if txn.NeedsReference() {
		found, err := r.Transactions().FindByExternal(ctx, txn.ProviderID(), txn.ReferenceExternalID())
		switch {
		case errors.Is(err, domain.ErrTransactionNotFound):
			return nil, p.awaitReference(txn, now, false)
		case err != nil:
			return nil, err
		}
		switch found.Status() {
		case domain.StatusPending, domain.StatusPendingReference:
			return nil, p.awaitReference(txn, now, true)
		case domain.StatusProcessed:
		default:
			// REJECTED ou FAILED: a referência nunca teve efeito a ser revertido.
			return reject(&domain.Rejection{
				Code:    domain.FailureReferenceNotProcessed,
				Message: fmt.Sprintf("referenced transaction ended as %s", found.Status()),
			})
		}
		ref = found
		if rej := txn.CheckReference(ref); rej != nil {
			return reject(rej)
		}
		// Uma referência aceita no máximo uma reversão bem-sucedida, de qualquer
		// tipo: REFUND e ROLLBACK da mesma BET devolveriam o mesmo débito duas vezes.
		// O índice único wager_tx_one_reversal_per_reference garante isso no banco.
		if txn.Kind().IsReversal() {
			done, err := r.Transactions().HasSuccessfulReversal(ctx, ref.ID())
			if err != nil {
				return nil, err
			}
			if done {
				return reject(&domain.Rejection{
					Code:    domain.FailureReferenceAlreadyReversed,
					Message: "referenced transaction already has a successful REFUND or ROLLBACK",
				})
			}
		}
	}

	var entry *domain.LedgerEntry
	if dir, moves := txn.MovementDirection(ref); moves {
		mv := domain.Movement{
			TransactionID: txn.ID(),
			Amount:        txn.Money(),
			Meta:          domain.EventMeta{CorrelationID: txn.CorrelationID(), CausationID: txn.ID().String(), OccurredAt: now},
		}
		var e domain.LedgerEntry
		var err error
		if dir == domain.DirectionDebit {
			e, err = wallet.Debit(mv)
		} else {
			e, err = wallet.Credit(mv)
		}
		// Código distinto para aposta sem saldo e para reversão sem saldo
		// (ROLLBACK de um WIN já gasto).
		if errors.Is(err, domain.ErrInsufficientFunds) {
			return reject(&domain.Rejection{Code: txn.InsufficientFundsCode(), Message: "balance is not enough to cover the debit"})
		}
		if err != nil {
			return nil, err
		}
		entry = &e
	} else if txn.Kind() != domain.KindLoss {
		return nil, fmt.Errorf("%w: %s has no movement", domain.ErrInvalidState, txn.Kind())
	}

	var refID *uuid.UUID
	if ref != nil {
		id := ref.ID()
		refID = &id
	}
	return entry, txn.MarkProcessed(now, wallet.Balance(), refID)
}

// awaitReference mantém txn esperando a referência ou, esgotado o limite de
// tentativas ou o TTL, rejeita. Referência que existe mas ainda está pendente
// (REFERENCE_UNAVAILABLE) é distinguida de referência que nunca chegou
// (REFERENCE_NOT_FOUND).
func (p *Processor) awaitReference(txn *domain.WagerTransaction, now time.Time, referenceExists bool) error {
	if txn.Status() == domain.StatusPending {
		return txn.WaitForReference(now, now.Add(Backoff(p.policy.BaseBackoff, p.policy.MaxBackoff, 0)), now.Add(p.policy.TTL))
	}
	p.metrics.Retry("reference_worker")
	expired := txn.ExpiresAt() != nil && !now.Before(*txn.ExpiresAt())
	if expired || txn.Attempts()+1 >= p.policy.MaxAttempts {
		if referenceExists {
			return txn.Reject(now, domain.FailureReferenceUnavailable,
				"referenced transaction did not reach a final state before the retry budget was exhausted", nil)
		}
		return txn.Reject(now, domain.FailureReferenceNotFound,
			"referenced transaction was not found before the retry budget was exhausted", nil)
	}
	return txn.RetryReference(now, now.Add(Backoff(p.policy.BaseBackoff, p.policy.MaxBackoff, txn.Attempts()+1)))
}

// persist grava tudo o que a operação produziu, na mesma transação SQL. A ordem
// respeita as foreign keys: transação, lançamento do ledger, saldo da carteira e
// eventos do outbox. Os eventos só serão publicados depois do commit.
func (p *Processor) persist(ctx context.Context, r Repositories, wallet *domain.Wallet, txn *domain.WagerTransaction, entry *domain.LedgerEntry, expectedVersion int64, isNew bool) error {
	var err error
	if isNew {
		err = r.Transactions().Insert(ctx, txn)
	} else {
		err = r.Transactions().Update(ctx, txn)
	}
	if err != nil {
		return err
	}
	if entry != nil {
		if err := r.Ledger().Insert(ctx, *entry); err != nil {
			return err
		}
		if err := r.Wallets().UpdateBalance(ctx, wallet, expectedVersion); err != nil {
			return err
		}
	}
	events := append(txn.PullEvents(), wallet.PullEvents()...)
	if err := r.Outbox().Add(ctx, events...); err != nil {
		return err
	}
	// Reversões que esperavam por esta transação ficam prontas para o worker agora,
	// sem aguardar o próximo intervalo de backoff.
	if txn.Status() == domain.StatusProcessed && txn.Origin() == domain.OriginExternal {
		return r.Transactions().WakeDependents(ctx, txn.ProviderID(), txn.ExternalTransactionID())
	}
	return nil
}

func (p *Processor) RecordOutcome(source Source, out Outcome, elapsed time.Duration) {
	t := out.Transaction
	p.metrics.ProcessingDuration(source, elapsed)
	if out.Replay {
		p.metrics.Duplicate(source, "replay")
		return
	}
	p.metrics.TransactionResult(source, t.Kind(), t.Status(), t.FailureCode())
}
