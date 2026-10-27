package event

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type OutboxWorker struct {
	repository 			OutboxRepository
	publisher 			Publisher
	retryDelaySeconds 	int
	pollIntervalSeconds int
}

func NewOutboxWorker(repository OutboxRepository, publisher Publisher, retryDelaySeconds int, pollIntervalSeconds int) *OutboxWorker {
	return &OutboxWorker{
		repository: 		repository,
		publisher: 			publisher,
		retryDelaySeconds: 	retryDelaySeconds,
		pollIntervalSeconds: pollIntervalSeconds,
	}
}

type OutboxRepository interface {
	ClaimNextOutboxEvent(ctx context.Context) (*OutboxEvent, error)
	MarkOutboxEventPublished(ctx context.Context, id uuid.UUID) error
	MarkOutboxEventFailed(ctx context.Context, id uuid.UUID, publishErr error, nextRetryAt time.Time) error
}

func (w *OutboxWorker) ProcessOne(ctx context.Context) error {
	outboxEvent, err := w.repository.ClaimNextOutboxEvent(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Println("No pending outbox events found")
			return nil
		}
		return err
	}

	if err := w.publisher.Publish(ctx, outboxEvent); err != nil {
		nextRetryAt := time.Now().Add(time.Duration(w.retryDelaySeconds) * time.Second)

		if markErr := w.repository.MarkOutboxEventFailed(ctx, outboxEvent.ID, err, nextRetryAt); markErr != nil {
			return markErr
		}
		return err
	}

	if err := w.repository.MarkOutboxEventPublished(ctx, outboxEvent.ID); err != nil {
		return err
	}

	return nil
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.pollIntervalSeconds) * time.Second)
	defer ticker.Stop()

	type result struct {
		err error
	}

	var (
		processing 	bool
		cancel 		context.CancelFunc
		resultCh	chan result
	)

	for {
		select {

		case <-ctx.Done():
			log.Println("Outbox worker shutting down...")

			if !processing {
				log.Println("Outbox worker stopped.")
				return
			}

			// Ask the active operation to stop
			cancel()

			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()

			select {
			case result := <-resultCh:
				if result.err != nil {
					log.Printf("Error processing outboxx event: %v", result.err)
				}
				log.Printf("Current operation finished.")
			case <- timer.C:
				log.Printf("Shutdown grace period expired")
			}
			log.Printf("Outbox worker stopped.")
			return

		case <-ticker.C:
			if processing {
				continue
			}

			processing = true
			resultCh = make(chan result, 1)
			
			operationCtx, operationCancel := context.WithCancel(ctx)
			cancel = operationCancel
			
			go func() {
				err := w.ProcessOne(operationCtx)
				resultCh <- result{err: err}
			}()

		case result := <-resultCh:
			processing = false
			cancel()
			cancel = nil
			resultCh = nil
			
			if result.err != nil {
				log.Printf("Error processing outbox event: %v", result.err)
			}
		}
	}
}