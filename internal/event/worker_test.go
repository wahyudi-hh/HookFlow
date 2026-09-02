package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type mockOutboxRepository struct {
	event            	*OutboxEvent
	getErr           	error
	markPublishedErr 	error
	publishedEventID	uuid.UUID

	failedEventID 		uuid.UUID
	failedError	 		error
	nextRetryAt			time.Time
}

const testRetryDelaySeconds = 30

type mockPublisher struct {
	err error
	publishedEvent *OutboxEvent
}

func (m *mockOutboxRepository) GetPendingOutboxEvents(ctx context.Context) (*OutboxEvent, error) {
	return m.event, m.getErr
}

func (m *mockOutboxRepository) MarkOutboxEventPublished(ctx context.Context, id uuid.UUID) error {
	m.publishedEventID = id
	return m.markPublishedErr
}

func (m *mockOutboxRepository) MarkOutboxEventFailed(
	ctx context.Context, id uuid.UUID, publishErr error, nextRetryAt time.Time) error {
		m.failedEventID = id
		m.failedError = publishErr
		m.nextRetryAt = nextRetryAt
		
		return nil
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

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("ProcessOne() returned error: %v", err)
	}

	if publisher.publishedEvent != outboxEvent {
		t.Fatal("Expected event to be published, but it was not")
	}

	if repository.publishedEventID != outboxEvent.ID {
		t.Fatalf(
			"Expected event ID %v to be marked as published, but got %v",
			outboxEvent.ID,
			repository.publishedEventID)
	}

}

func TestOutboxWorker_ProcessOne_NoPendingEvents(t *testing.T) {
	repository := &mockOutboxRepository{
		getErr: pgx.ErrNoRows,
	}

	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}

	if publisher.publishedEvent != nil {
		t.Fatal("Expected no event to be published, but an event was published")
	}

	if repository.publishedEventID != uuid.Nil {
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

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds)

	before := time.Now()

	err := worker.ProcessOne(context.Background())

	after := time.Now()

	if err == nil {
		t.Fatal("Expected error, but got none")
	}

	if repository.failedEventID != outboxEvent.ID {
		t.Fatalf("Expected worker to mark the same event as failed")
	}

	if !errors.Is(repository.failedError, publisher.err) {
		t.Fatalf("Expected error %v, got %v", repository.failedError, publisher.err)
	}

	expectedMin := before.Add(time.Duration(testRetryDelaySeconds) * time.Second)
	expectedMax := after.Add(time.Duration(testRetryDelaySeconds) * time.Second)

	if repository.nextRetryAt.Before(expectedMin) || repository.nextRetryAt.After(expectedMax) {
		t.Fatalf(
			"Expected next retry time to be between %v and %v, but got %v",
			expectedMin,
			expectedMax,
			repository.nextRetryAt)
	}

	if repository.publishedEventID != uuid.Nil {
		t.Fatalf("Expected no event to be marked as published")
	}
}