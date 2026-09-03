package event

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

type OutboxEvent struct {
	ID      		uuid.UUID
	EventID 		uuid.UUID
	Topic   		string
	AttemptCount 	int
	NextRetryAt   	*time.Time

	ClientID 		uuid.UUID
	ClientEventID 	string
	Type 			string
	Payload 		[]byte
}

func (r *Repository) CreateEvent(ctx context.Context, event Event) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, 
		`INSERT INTO events (id, type, client_id, client_event_id, payload)
		VALUES ($1, $2, $3, $4, $5)`,
		event.ID, event.Type, event.ClientID, event.ClientEventId, event.Payload)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrorDuplicateEvent
		}
		return fmt.Errorf("failed to insert event: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox_events (id, event_id, topic, status)
		VALUES ($1, $2, $3, $4)`,
		uuid.New(), event.ID, "events", "PENDING")
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *Repository) GetPendingOutboxEvents(ctx context.Context) (*OutboxEvent, error) {
	var outboxEvent OutboxEvent

	err := r.db.QueryRow(ctx, `
		SELECT 
			o.id, 
			o.event_id, 
			o.topic, 
			o.attempt_count,
			o.next_retry_at,
			e.client_id,
			e.client_event_id,
			e.type,
			e.payload
		FROM outbox_events o
		JOIN events e ON o.event_id = e.id
		WHERE o.status = 'PENDING'
			AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW())
		ORDER BY e.created_at
		LIMIT 1`,
	).Scan(
		&outboxEvent.ID,
		&outboxEvent.EventID,
		&outboxEvent.Topic,
		&outboxEvent.AttemptCount,
		&outboxEvent.NextRetryAt,
		&outboxEvent.ClientID,
		&outboxEvent.ClientEventID,
		&outboxEvent.Type,
		&outboxEvent.Payload,
	)

	if err != nil {
		return nil, err
	}
	
	return &outboxEvent, nil
}

func (r *Repository) MarkOutboxEventPublished(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.Exec(ctx, 
		`UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = NOW()
		WHERE id = $1`,
		id,
	)
	return err
}

func (r *Repository) MarkOutboxEventFailed(
	ctx context.Context, id uuid.UUID, publishErr error, nextRetryAt time.Time) error {
	_, err := r.db.Exec(ctx,
		`UPDATE outbox_events
		SET status = 'PENDING', attempt_count = attempt_count + 1, last_error = $2, next_retry_at = $3
		WHERE id = $3`,
		id, publishErr.Error(), nextRetryAt,
	)
	return err
}