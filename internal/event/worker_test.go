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
	claimStarted 		chan		struct{}
	markFailedErr 		error
	failedEventCh 		chan struct{}
}

const testRetryDelaySeconds = 30
const testPollIntervalSeconds = 5

type mockPublisher struct {
	err error
	publishedEvent *OutboxEvent
}

func (m *mockOutboxRepository) ClaimNextOutboxEvent(ctx context.Context) (*OutboxEvent, error) {
	if m.claimStarted != nil {
		close(m.claimStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}
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
		
		if m.failedEventCh != nil {
			close(m.failedEventCh)
		}
		return m.markFailedErr
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

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("ProcessOne() returned error: %v", err)
	}

	if publisher.publishedEvent != outboxEvent {
		t.Fatal("expected event to be published, but it was not")
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

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

	err := worker.ProcessOne(context.Background())

	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}

	if publisher.publishedEvent != nil {
		t.Fatal("expected no event to be published, but an event was published")
	}

	if repository.publishedEventID != uuid.Nil {
		t.Fatalf("expected no event to be marked as published")
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

	worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

	before := time.Now()

	err := worker.ProcessOne(context.Background())

	after := time.Now()

	if err == nil {
		t.Fatal("expected error, but got none")
	}

	if repository.failedEventID != outboxEvent.ID {
		t.Fatalf("expected worker to mark the same event as failed")
	}

	if !errors.Is(repository.failedError, publisher.err) {
		t.Fatalf("expected error %v, got %v", repository.failedError, publisher.err)
	}

	expectedMin := before.Add(time.Duration(testRetryDelaySeconds) * time.Second)
	expectedMax := after.Add(time.Duration(testRetryDelaySeconds) * time.Second)

	if repository.nextRetryAt.Before(expectedMin) || repository.nextRetryAt.After(expectedMax) {
		t.Fatalf(
			"expected next retry time to be between %v and %v, but got %v",
			expectedMin,
			expectedMax,
			repository.nextRetryAt)
	}

	if repository.publishedEventID != uuid.Nil {
		t.Fatalf("expected no event to be marked as published")
	}
}

func TestOutboxWorker_ProcessOne_MarkFailedFails(t *testing.T) {
    outboxEvent := newTestOutboxEvent()
    publishErr := errors.New("publish failed")
    markFailedErr := errors.New("database unavailable")

    repository := &mockOutboxRepository{
        event:         outboxEvent,
        markFailedErr: markFailedErr,
    }
    publisher := &mockPublisher{err: publishErr}

    worker := NewOutboxWorker(repository, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

    err := worker.ProcessOne(context.Background())

    if !errors.Is(err, markFailedErr) {
        t.Fatalf("expected error %v, got %v", markFailedErr, err)
    }

    if !errors.Is(repository.failedError, publishErr) {
        t.Fatalf("expected original publish error %v, got %v",
            publishErr, repository.failedError)
    }
}

func TestOutboxWorker_ProcessOne_MarkPublishedFails(t *testing.T) {
	outboxEvent := newTestOutboxEvent()
	markPublishedErr := errors.New("database unavailable")

	repo := &mockOutboxRepository{
		event: outboxEvent,
		markPublishedErr:  markPublishedErr,
	}

	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repo, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

	err := worker.ProcessOne(context.Background())

	if !errors.Is(err, markPublishedErr) {
		t.Fatalf("expected error %v, got %v", markPublishedErr, err)
	}
	if publisher.publishedEvent != outboxEvent {
        t.Fatal("expected event to be published before marking it")
    }
}

func TestOutboxWorker_Run_ShutdownWhileIdle(t *testing.T) {
	repo := &mockOutboxRepository{}
	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repo, publisher, testRetryDelaySeconds, testPollIntervalSeconds)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func ()  {
		defer close(done)
		worker.Run(ctx)
	}()

	// Allow Run() to start before requesting shutdown.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Run return successfully
	case <-time.After(time.Second):
		t.Fatalf("Run() didn't stop promptly ater cancellation while idle.")
	}

	if publisher.publishedEvent != nil {
		t.Fatal("expected no event to be published during idle shutdown")
	}
}

func TestOutboxWorker_Run_ShutdownDuringProcessing(t *testing.T) {
	repo := &mockOutboxRepository{
		claimStarted: make(chan struct{}),
	}
	publisher := &mockPublisher{}
	worker := NewOutboxWorker(repo, publisher, testRetryDelaySeconds, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func ()  {
		defer close(done)
		worker.Run(ctx)
	}()

	// Wait until ProcessOne has started its repository call.
	select {
	case <-repo.claimStarted:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatalf("worker never started processing.")
	}

	// Request shutdown while processing is active.
	cancel()

	select {
	case <-done:
		// Run returned after the operation observed cancellation.
	case <-time.After(2 * time.Second):
		t.Fatalf("worker didn't stop after cancellation.")
	}

	if publisher.publishedEvent != nil {
		t.Fatal("expected no event to be published")
	}
}

func TestOutboxWorker_Run_ContinuesAfterProcessingError(t *testing.T) {
	event := newTestOutboxEvent()
	publishErr := errors.New("publish failed")

	repo := &mockOutboxRepository{
		event: event,
		failedEventCh: make(chan struct{}),
	}
	publisher := &mockPublisher{err: publishErr}

	worker := NewOutboxWorker(repo, publisher, testRetryDelaySeconds, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func ()  {
		defer close(done)
		worker.Run(ctx)
	}()

	// Wait until the worker has attempted to publish the event.
	for repo.failedEventID != event.ID {
		select {
		case <-repo.failedEventCh:
		case <-time.After(3 * time.Second):
			cancel()
			<-done
			t.Fatal("worker did not attempt to process the event")
		}
	}

	// Give Run() time to receive the result and continue polling.
    time.Sleep(100 * time.Millisecond)
    cancel()

    select {
    case <-done:
    case <-time.After(2 * time.Second):
        t.Fatal("worker did not stop after cancellation")
    }
}

func TestOutboxWorker_Run_SkipTickWhileProcessing(t *testing.T) {
	repo := &mockOutboxRepository{claimStarted: make(chan struct{})}
	publisher := &mockPublisher{}

	worker := NewOutboxWorker(repo, publisher, testRetryDelaySeconds, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func ()  {
		defer close(done)
		worker.Run(ctx)
	}()

	// Wait for the first processing attempt to start.
	select {
	case <-repo.claimStarted:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatalf("worker didn't start processing")
	}

	// Allow another ticker interval to pass while processing is blocked.
    time.Sleep(1200 * time.Millisecond)

    // Stop the worker and wait for it to exit.
    cancel()

    select {
    case <-done:
    case <-time.After(2 * time.Second):
        t.Fatal("worker did not stop after cancellation")
    }
}