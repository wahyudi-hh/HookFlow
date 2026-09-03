package main

import (
	"context"
	"log"
	"os"
	"os/signal"

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
	worker := event.NewOutboxWorker(repository, publisher, cfg.Outbox.RetryDelaySeconds, cfg.Outbox.PollIntervalSeconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	
	go func() {
		<-signalChan
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	}()

	worker.Run(ctx)
}
