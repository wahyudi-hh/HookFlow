package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/config"
	"github.com/wahyudi-hh/HookFlow/internal/db"
	"github.com/wahyudi-hh/HookFlow/internal/event"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file!")
	}

	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	pool, err := db.NewPostgresPool(context.Background(), cfg.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repository := event.NewRepository(pool)
	publisher := event.NewLogPublisher()
	worker := event.NewOutboxWorker(repository, publisher)

	if err := worker.ProcessOne(context.Background()); err != nil {
		log.Fatal(err)
	}
}
