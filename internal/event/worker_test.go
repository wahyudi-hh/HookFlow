package event

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type mockOutboxRepository struct {
	event            *OutboxEvent
	getErr           error
	markPublishedErr error
	publishedEventId uuid.UUID
}

type mockPublisher struct {
	err error
	publishedEvent *OutboxEvent
}

func (m *mockOutboxRepository) GetPendingOutboxEvents(ctx context.Context) (*OutboxEvent, error) {
	return m.event, m.getErr
}

func (m *mockOutboxRepository) MarkOutboxEventPublished(ctx context.Context, id uuid.UUID) error {
	m.publishedEventId = id
	return m.markPublishedErr
}

func (m *mockPublisher) Publish(ctx context.Context, event *OutboxEvent) error {
	m.publishedEvent = event
	return m.err
}

func newTestOutboxEvent() *OutboxEvent {
	return &OutboxEvent{
		ID:      uuid.New(),
		EventID: uuid.New(),
		Topic:   "events",
		Payload: []byte(`{"order_id": "order-123"}`),
	}
}

func TestOutboxWorker_ProcessOne(t *testing.T) {	
	outboxEvent := newTestOutboxEvent();

	repository := &mockOutboxRepository{
		event: outboxEvent,
	}

	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repository, publisher)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("ProcessOne() returned error: %v", err)
	}

	if publisher.publishedEvent != outboxEvent {
		t.Fatal("Expected event to be published, but it was not")
	}

	if repository.publishedEventId != outboxEvent.ID {
		t.Fatalf(
			"Expected event ID %v to be marked as published, but got %v",
			outboxEvent.ID,
			repository.publishedEventId)
	}

}

func TestOutboxWorker_ProcessOne_NoPendingEvents(t *testing.T) {
	repository := &mockOutboxRepository{
		getErr: pgx.ErrNoRows,
	}

	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repository, publisher)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}

	if publisher.publishedEvent != nil {
		t.Fatal("Expected no event to be published, but an event was published")
	}

	if repository.publishedEventId != uuid.Nil {
		t.Fatalf("Expected no event to be marked as published")
	}
}

func TestOutboxWorker_ProcessOne_PublishFails(t *testing.T) {
	outboxEvent := newTestOutboxEvent();

	repository := &mockOutboxRepository{
		event: outboxEvent,
	}

	publisher := &mockPublisher{
		err: errors.New("publish failed"),
	}

	worker := NewOutboxWorker(repository, publisher)

	err := worker.ProcessOne(context.Background())

	if err == nil {
		t.Fatal("Expected error, but got none")
	}

	if repository.publishedEventId != uuid.Nil {
		t.Fatalf("Expected no event to be marked as published")
	}
}