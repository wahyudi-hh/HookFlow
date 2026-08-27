package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/auth"
	"github.com/wahyudi-hh/HookFlow/internal/client"
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

	eventRepository := event.NewRepository(pool)
	eventService := event.NewService(eventRepository)
	eventHandler := event.NewHandler(eventService)

	clientRepository := client.NewRepository(pool)
	appSecret := os.Getenv("APP_SECRET_KEY")
	if appSecret == "" {
		log.Fatal("APP_SECRET_KEY is required")
	}
	authenticator := auth.NewAuthenticator(clientRepository, appSecret)

	http.HandleFunc("GET /health", healthHandler)
	http.Handle("POST /v1/events", authenticator.Middleware(http.HandlerFunc(eventHandler.CreateEvent)))

	fmt.Println("API server listening on :8080")

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic(err)
	}
}