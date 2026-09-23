package main

import (
	"context"
	"log"

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

	message, err := consumer.Read(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received message: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
		message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value),
	)
}