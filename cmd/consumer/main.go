package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"

	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/config"
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

	if err := cfg.Validate(); err!=nil {
		log.Fatal(err)
	}

	consumer := event.NewKafkaConsumer(cfg.Kafka.Broker, cfg.Kafka.Topic, cfg.Kafka.ConsumerGroup)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func ()  {
		<- signalChan
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	}()

	logger := log.Default()
	handler := event.NewLoggingEventHandler(logger)

	for {
		message, err := consumer.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}

			log.Printf("Failed to fetch message: %v", err)
			break
		}

		log.Printf("Message fetched: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value),
		)

		if err := handler.Handle(ctx, message); err != nil {
			log.Printf("Failed to process message: %v", err)
        	continue
		}

		if err := consumer.Commit(ctx, message); err != nil {
			log.Printf("Failed to commit message: %v", err)
        	continue
		}

		log.Printf("Message commited: key=%s", string(message.Key))
	}

	if err := consumer.Close(); err != nil {
		log.Printf("Failed to close Kafka consumer: %v", err)
	}
}