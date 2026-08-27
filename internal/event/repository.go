package event

import (
	"context"
	"errors"
	"fmt"

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