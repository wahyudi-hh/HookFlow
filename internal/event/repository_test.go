package event

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/config"
	"github.com/wahyudi-hh/HookFlow/internal/db"
)

func TestRepository_MarkEventProcessed(t *testing.T) {
	if err := godotenv.Load("../../.env"); err != nil {
		t.Fatalf("failed to load .env: %v", err)
	}

	cfg, err := config.Load("../../config/config.yaml")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("invalid config: %v", err)
	}
	
	ctx := context.Background()

	pool, err := db.NewPostgresPool(ctx, cfg.Database)
	if err != nil {
		t.Fatalf("failed to create db pool: %v", err)		
	}
	defer pool.Close()

	repo := NewRepository(pool)
	eventID := uuid.New()

	// First transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)		
	}
	defer tx.Rollback(ctx)

	processed, err := repo.MarkEventProcessed(ctx, tx, eventID)
	if err != nil {
		t.Fatalf("failed to mark event as processed: %v", err)
	}
	if !processed {
		t.Fatalf("expected event to be marked as processed, but failed")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)		
	}

	// Second transaction, same event ID
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin second transaction: %v", err)		
	}
	defer tx.Rollback(ctx)
	
	processed, err = repo.MarkEventProcessed(ctx, tx, eventID)
	if err != nil {
		t.Fatalf("failed to mark second event as processed: %v", err)
	}
	if processed {
		t.Fatalf("expected duplicate event to be ignored, but marked as processed")
	}
}