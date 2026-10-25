package event

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type EventHandler interface {
	Handle(ctx context.Context, message kafka.Message) error
}

type EventTransaction interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type EventDatabase interface {
	Begin(ctx context.Context) (EventTransaction, error)
}

type ProcessedEventRepository interface {
	MarkEventProcessed(
		ctx context.Context,
		tx ProcessedEventTx,
		eventID uuid.UUID,
	) (bool, error)
}

type PostgresEventTransaction struct {
	tx pgx.Tx
}

func (t *PostgresEventTransaction) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, arguments...)
}

func (t *PostgresEventTransaction) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *PostgresEventTransaction) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}
 
type PostgresEventDatabase struct {
	pool *pgxpool.Pool
}

func NewPostgresEventDatabase(pool *pgxpool.Pool) *PostgresEventDatabase {
	return &PostgresEventDatabase{pool: pool}
}

func (d *PostgresEventDatabase) Begin(ctx context.Context) (EventTransaction, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	
	return &PostgresEventTransaction{tx: tx}, nil
}

type IdempotentEventHandler struct {
	db EventDatabase
	repo ProcessedEventRepository
	logger *log.Logger
}

func NewIdempotentEventHandler(
	db EventDatabase,
	repo ProcessedEventRepository,
	logger *log.Logger,
) *IdempotentEventHandler {
	return &IdempotentEventHandler{
		db: db,
		repo: repo,
		logger: logger,
	}
}

func (h *IdempotentEventHandler) Handle(ctx context.Context, message kafka.Message) error {
	// The Kafka message key contains our internal event UUID.
	eventId, err := uuid.Parse(string(message.Key))
	if err != nil {
		return fmt.Errorf("invalid event ID in message key %q: %w", message.Key, err)
	}

	tx, err := h.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	processed, err := h.repo.MarkEventProcessed(ctx, tx, eventId)
	if err != nil {
		return fmt.Errorf("mark event as processed: %w", err)		
	}

	if !processed {
		h.logger.Printf("Skipping duplicate event: event ID=%s", eventId)
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit duplicate event transaction: %w", err)
		}
		return nil
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commiting event process transaction: %w", err)
	}

	h.logger.Printf("Processed event: event id: %s", eventId)
	return nil
}