package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/config"
	"github.com/wahyudi-hh/HookFlow/internal/db"
	"github.com/wahyudi-hh/HookFlow/internal/event"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status": "ok"}`)
}

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"Database: %s:%d/%s",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Name,
	)

	ctx := context.Background()
	pool, err := db.NewPostgresPool(ctx, cfg.Database)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repository := event.NewRepository(pool)
	service := event.NewService(repository)
	handler := event.NewHandler(service)

	http.HandleFunc("GET /health", healthHandler)
	http.HandleFunc("POST /v1/events", handler.CreateEvent)

	fmt.Println("API server listening on :8080")

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}