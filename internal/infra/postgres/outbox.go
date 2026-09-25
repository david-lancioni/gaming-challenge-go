package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dlancioni/backend-challenge-go/internal/application"
	"github.com/dlancioni/backend-challenge-go/internal/domain"
)

type outboxRepo struct{ db dbtx }

// Add grava o envelope público completo do evento, serializado uma única vez: toda
// republicação envia exatamente os mesmos bytes, com o mesmo eventId.
func (r *outboxRepo) Add(ctx context.Context, events ...domain.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal event %s: %w", e.ID(), err)
		}
		_, err = r.db.Exec(ctx, `INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, partition_key, event_type, event_version, payload, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			e.ID(), e.AggregateType(), e.AggregateID(), e.PartitionKey(), string(e.Type()), e.Version(), payload, e.OccurredAt())
		if err != nil {
			return err
		}
	}
	return nil
}

type inboxRepo struct{ db dbtx }

func (r *inboxRepo) Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (bool, string, error) {
	tag, err := r.db.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, hash, now)
	if err != nil {
		return false, "", err
	}
	if tag.RowsAffected() == 1 {
		return true, hash, nil
	}
	var existing string
	err = r.db.QueryRow(ctx, `SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&existing)
	return false, existing, err
}

func (r *inboxRepo) Complete(ctx context.Context, consumer, messageID string, now time.Time) error {
	tag, err := r.db.Exec(ctx, `UPDATE inbox_messages SET completed_at = $3
		WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NULL`, consumer, messageID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrConcurrentModification
	}
	return nil
}

type outboxQueue struct{ pool *pgxpool.Pool }

func NewOutboxQueue(pool *pgxpool.Pool) application.OutboxQueue { return &outboxQueue{pool: pool} }

// Claim reserva eventos pendentes para um publisher:
//
//   - FOR UPDATE SKIP LOCKED faz publishers concorrentes receberem linhas
//     disjuntas, sem esperar uns pelos outros;
//   - o lease (locked_until) devolve à fila, quando expira, as linhas de um
//     publisher que morreu (recuperação de trabalho abandonado);
//   - cada claim conta uma tentativa, então uma queda entre o claim e a
//     confirmação fica visível em attempts;
//   - só o evento pendente mais antigo de cada carteira (partition_key) é
//     elegível, então os eventos de uma carteira nunca saem fora de ordem, mesmo
//     com vários publishers ou com um evento anterior aguardando retry. O
//     MessageGroupId da fila FIFO (a carteira) preserva essa ordem até o consumidor.
func (q *outboxQueue) Claim(ctx context.Context, workerID string, limit int, lease time.Duration) ([]application.OutboxRecord, error) {
	rows, err := q.pool.Query(ctx, `
		WITH cand AS (
			SELECT o.id FROM outbox_events o
			 WHERE o.published_at IS NULL AND o.next_attempt_at <= now()
			   AND (o.locked_until IS NULL OR o.locked_until < now())
			   AND NOT EXISTS (SELECT 1 FROM outbox_events e
			                    WHERE e.partition_key = o.partition_key AND e.published_at IS NULL AND e.seq < o.seq)
			 ORDER BY o.seq
			 LIMIT $1
			 FOR UPDATE OF o SKIP LOCKED)
		UPDATE outbox_events o
		   SET locked_by = $2, locked_until = now() + $3::bigint * interval '1 millisecond', attempts = o.attempts + 1
		  FROM cand
		 WHERE o.id = cand.id
		RETURNING o.seq, o.id, o.event_type, o.aggregate_id, o.partition_key, o.payload::text, o.occurred_at, o.attempts`,
		limit, workerID, lease.Milliseconds())
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	type seqRecord struct {
		seq int64
		application.OutboxRecord
	}
	var recs []seqRecord
	for rows.Next() {
		var s seqRecord
		var payload string
		if err := rows.Scan(&s.seq, &s.ID, &s.EventType, &s.AggregateID, &s.PartitionKey, &payload, &s.OccurredAt, &s.Attempts); err != nil {
			return nil, mapErr(err)
		}
		s.Payload = []byte(payload)
		recs = append(recs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, mapErr(err)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].seq < recs[j].seq })
	out := make([]application.OutboxRecord, len(recs))
	for i, s := range recs {
		out[i] = s.OutboxRecord
	}
	return out, nil
}

func (q *outboxQueue) MarkPublished(ctx context.Context, id uuid.UUID) error {
	_, err := q.pool.Exec(ctx, `UPDATE outbox_events
		SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = $1 AND published_at IS NULL`, id)
	return mapErr(err)
}

func (q *outboxQueue) MarkFailed(ctx context.Context, id uuid.UUID, cause string, retryAfter time.Duration) error {
	if len(cause) > 500 {
		cause = cause[:500]
	}
	_, err := q.pool.Exec(ctx, `UPDATE outbox_events
		SET last_error = $2, locked_by = NULL, locked_until = NULL,
		    next_attempt_at = now() + $3::bigint * interval '1 millisecond'
		WHERE id = $1 AND published_at IS NULL`, id, cause, retryAfter.Milliseconds())
	return mapErr(err)
}

func (q *outboxQueue) OldestUnpublishedAge(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := q.pool.QueryRow(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)::float8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&seconds)
	if err != nil {
		return 0, mapErr(err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
