package event

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"github.com/wahyudi-hh/HookFlow/internal/config"
	"github.com/wahyudi-hh/HookFlow/internal/db"
)

type fakeEventDatabase struct {
	tx 	EventTransaction
	err error
}

func (f *fakeEventDatabase) Begin(ctx context.Context) (EventTransaction, error) {
	return f.tx, f.err
}

type fakeEventTransaction struct {
	execErr 	error
	commitErr 	error
	rollbackErr error
}

func (f *fakeEventTransaction) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), f.execErr
}

func (f *fakeEventTransaction) Commit(ctx context.Context) error {
	return f.commitErr
}

func (f *fakeEventTransaction) Rollback(ctx context.Context) error {
	return f.rollbackErr
}

type fakeProcessedEventRepository struct {
	processed 	bool
	err	 		error
}

func (f *fakeProcessedEventRepository) MarkEventProcessed (
	ctx context.Context,
	tx ProcessedEventTx,
	eventID uuid.UUID,
) (bool, error) {
	return f.processed, f.err
}

func TestIdempotentEventHandler_Handle(t *testing.T) {
	if err := godotenv.Load("../../.env"); err != nil {
		t.Fatalf("load .env: %v", err)
	}

	cfg, err := config.Load("../../config/config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	ctx := context.Background()
	pool, err := db.NewPostgresPool(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("create Postgres poll: %v", err)
	}
	defer pool.Close()

	repo := NewRepository(pool)
	handler := NewIdempotentEventHandler(NewPostgresEventDatabase(pool), repo, log.New(os.Stdout, "", log.LstdFlags))

	eventID := uuid.New()
	message := kafka.Message {
		Key: []byte(eventID.String()),
		Value: []byte(`{"amount": 100}`),
	}

	// First delivery should be processed
	if err := handler.Handle(ctx, message); err != nil {
		t.Fatalf("handle first message: %v", err)
	}

	// Second delivery should be recognized as duplicate
	if err := handler.Handle(ctx, message); err != nil {
		t.Fatalf("handle duplicate message: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`,
		eventID,
	).Scan(&count); err!= nil {
		t.Fatalf("query count processed event: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 processed event, got %d", count)
	}
}

func TestIdempotentEventHandler_Handle_invalidEventID(t *testing.T) {
	handler := NewIdempotentEventHandler(nil, nil, log.New(os.Stdout, "", log.LstdFlags))

	message := kafka.Message {
		Key: []byte("invalid-event-ID"),
	}

	err := handler.Handle(context.Background(), message)
	if err == nil {
		t.Fatalf("expected an error for invalid event ID, got nil.")
	}

	if !strings.Contains(err.Error(), "invalid event ID in message key") {
		t.Fatalf(`expected "invalid event ID in message key ..." error, got: %v`, err)
	}
}

func TestIdempotentEventHandler_Handle_BeginError(t *testing.T) {
	expectedErr := errors.New("database unavailable")

	handler := NewIdempotentEventHandler(
		&fakeEventDatabase{err: expectedErr},
		&fakeProcessedEventRepository{},
		log.New(io.Discard, "", 0),
	)

	message := kafka.Message{Key: []byte(uuid.NewString())}

	err := handler.Handle(context.Background(), message)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped begin error, got %v", err)
	}
}

func TestIdempotentEventHandler_Handle_MarkProcessedError(t *testing.T) {
	expectedErr := errors.New("insert failed")
	tx := &fakeEventTransaction{}

	handler := NewIdempotentEventHandler(
		&fakeEventDatabase{tx: tx},
		&fakeProcessedEventRepository{err: expectedErr},
		log.New(io.Discard, "", 0),
	)

	message := kafka.Message{Key: []byte(uuid.NewString())}

	err := handler.Handle(context.Background(), message)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped processing error, got %v", err)
	}

	if tx.commitErr != nil {
		t.Fatal("unexpected commit error configuration")
	}
}

func TestIdempotentEventHandler_Handle_CommitError(t *testing.T) {
	expectedErr := errors.New("commit failed")
	tx := &fakeEventTransaction{commitErr: expectedErr}

	handler := NewIdempotentEventHandler(
		&fakeEventDatabase{tx: tx},
		&fakeProcessedEventRepository{processed: true},
		log.New(io.Discard, "", 0),
	)

	message := kafka.Message{Key: []byte(uuid.NewString())}

	err := handler.Handle(context.Background(), message)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped commit error, got %v", err)
	}
}

func TestIdempotentEventHandler_Handle_DuplicateCommitError(t *testing.T) {
	expectedErr := errors.New("duplicate commit failed")
	tx := &fakeEventTransaction{commitErr: expectedErr}

	handler := NewIdempotentEventHandler(
		&fakeEventDatabase{tx: tx},
		&fakeProcessedEventRepository{processed: false},
		log.New(io.Discard, "", 0),
	)

	message := kafka.Message{Key: []byte(uuid.NewString())}

	err := handler.Handle(context.Background(), message)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped duplicate commit error, got %v", err)
	}
}