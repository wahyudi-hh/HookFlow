package event

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type EventHandler interface {
	Handle(ctx context.Context, message kafka.Message) error
}

type LoggingEventHandler struct {
	logger *log.Logger
}

func NewLoggingEventHandler(logger *log.Logger) *LoggingEventHandler {
	return &(LoggingEventHandler{
		logger: logger,
	})
}

type IdempotentEventHandler struct {
	db *pgxpool.Pool
	repo *Repository
	logger *log.Logger
}

func NewIdempotentEventHandler(db *pgxpool.Pool, repo *Repository, logger *log.Logger) *IdempotentEventHandler {
	return &IdempotentEventHandler{
		db: db,
		repo: repo,
		logger: logger,
	}
}

func (h *LoggingEventHandler) Handle(ctx context.Context, message kafka.Message) error {
	h.logger.Printf(
		"Processing event: topic=%s partition=%d offset=%d key=%s value=%s",
        message.Topic,
        message.Partition,
        message.Offset,
        string(message.Key),
        string(message.Value),
	)

	return nil
}

func (h *IdempotentEventHandler) Handle(ctx context.Context, message kafka.Message) error {
	// The Kafka message key contains our internal event UUID.
	eventId, err := uuid.Parse(string(message.Key))
	if err != nil {
		return fmt.Errorf("Invalid event ID in message key %q: %w", message.Key, err)
	}

	tx, err := h.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	processed, err := h.repo.MarkEventProcessed(ctx, tx, eventId)
	if err != nil {
		return fmt.Errorf("Error mark event as processed: %w", err)		
	}

	if !processed {
		h.logger.Printf("Skipping duplicate event: event ID=%s", eventId)
		return tx.Commit(ctx)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("Error commiting event process transaction: %w", err)
	}

	h.logger.Printf("Processed event: event id: %s", eventId)
	return nil
}