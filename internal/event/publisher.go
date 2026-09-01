package event

import (
	"context"
	"log"
)

type Publisher interface {
	Publish(ctx context.Context, event *OutboxEvent) error
}

type LogPublisher struct{}

func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

func (p *LogPublisher) Publish(ctx context.Context, event *OutboxEvent) error {
	log.Printf(
		"Publishing event: event_id= %s — topic= %s — payload= %s",
		event.EventID,
		event.Topic,
		string(event.Payload),
	)

	return nil
}