package event

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestKafkaPublisher_Publish(t *testing.T) {
	publisher := NewKafkaPublisher("localhost:9092", "hookflow.events")

	event := &OutboxEvent{
		EventID: uuid.New(),
		Payload: []byte(`{"message" : "Hello from HookFlow!"}`),
	}

	err := publisher.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
}
