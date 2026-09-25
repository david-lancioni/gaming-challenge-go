package application

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

const maxDuplicateRetries = 3

type WageringService struct {
	uow     UnitOfWork
	reader  TransactionReader
	proc    *Processor
	metrics Metrics
	log     *slog.Logger
}

func NewWageringService(uow UnitOfWork, reader TransactionReader, proc *Processor, metrics Metrics, log *slog.Logger) *WageringService {
	if metrics == nil {
		metrics = NopMetrics{}
	}
	return &WageringService{uow: uow, reader: reader, proc: proc, metrics: metrics, log: log}
}

func (s *WageringService) Submit(ctx context.Context, source Source, params domain.ExternalParams) (Outcome, error) {
	start := time.Now()
	probe, err := domain.NewExternalTransaction(params, s.proc.clock())
	if err != nil {
		return Outcome{}, err
	}

	// Atalho: um replay é respondido sem pegar o lock da carteira. A verificação
	// que garante a idempotência é a do Processor, já sob lock.
	existing, err := s.reader.FindByProviderKeys(ctx, probe.ProviderID(), probe.IdempotencyKey(), probe.ExternalTransactionID())
	if err != nil {
		return Outcome{}, err
	}
	if len(existing) > 0 {
		prior, err := ClassifyExisting(probe, existing)
		if err != nil {
			s.recordConflict(source, err)
			return Outcome{}, err
		}
		out := Outcome{Transaction: prior, Replay: true}
		s.proc.RecordOutcome(source, out, time.Since(start))
		return out, nil
	}

	var out Outcome
	for attempt := 0; ; attempt++ {
		err = s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
			var perr error
			out, perr = s.proc.ProcessExternal(ctx, r, params)
			return perr
		})
		// Perdeu a corrida de INSERT para uma requisição concorrente com a mesma
		// chave ou id externo (possivelmente em outra carteira, com outro lock).
		// As constraints únicas barraram a duplicata; reavalia para devolver o
		// replay ou o conflito correto.
		if errors.Is(err, ErrDuplicateTransaction) && attempt < maxDuplicateRetries {
			s.metrics.ConcurrencyConflict("duplicate_insert")
			continue
		}
		break
	}
	if err != nil {
		s.recordConflict(source, err)
		return Outcome{}, err
	}
	s.proc.RecordOutcome(source, out, time.Since(start))
	return out, nil
}

func (s *WageringService) recordConflict(source Source, err error) {
	if errors.Is(err, domain.ErrIdempotencyConflict) || errors.Is(err, domain.ErrExternalTransactionConflict) {
		s.metrics.Duplicate(source, "conflict")
	}
}

func (s *WageringService) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return s.reader.GetByID(ctx, id)
}

func (s *WageringService) GetProviderTransaction(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return s.reader.FindByExternal(ctx, providerID, externalID)
}

func (s *WageringService) Processor() *Processor { return s.proc }
