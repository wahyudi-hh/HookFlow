package event

import (
	"context"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
)

type OutboxWorker struct {
	repository *Repository
	publisher Publisher
}

func NewOutboxWorker(repository *Repository, publisher Publisher) *OutboxWorker {
	return &OutboxWorker{
		repository: repository,
		publisher: publisher,
	}
}

func (w *OutboxWorker) ProcessOne(ctx context.Context) error {
	outboxEvent, err := w.repository.GetPendingOutboxEvents(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Println("No pending outbox events found")
			return nil
		}
		return err
	}

	if err := w.publisher.Publish(ctx, outboxEvent); err != nil {
		return err
	}

	if err := w.repository.MarkOutboxEventPublished(ctx, outboxEvent.ID); err != nil {
		return err
	}

	return nil
}