package event

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokerAdress, topic string) *KafkaPublisher {
	writer := &kafka.Writer{
		Addr: kafka.TCP(brokerAdress),
		Topic: topic,
	}

	return &KafkaPublisher{
		writer: writer,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event *OutboxEvent) error {
	message := kafka.Message{
		Key: []byte(event.EventID.String()),
		Value: event.Payload,
	}
	err := p.writer.WriteMessages(ctx, message)
	if err != nil {
		log.Printf("Error while publishing outbox event: event_id=%s topic=%s", event.EventID, event.Topic)
		return err
	}
	log.Printf("Published outbox event: event_id=%s topic=%s", event.EventID, event.Topic)
	return err
}