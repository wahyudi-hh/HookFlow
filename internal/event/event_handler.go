package event

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type EventHandler interface {
	Handle(ctx context.Context, message kafka.Message) error
}

type LoggingEventHandler struct {
	logger *log.Logger
}

func NewLoggingEventHandler(logger *log.Logger) *LoggingEventHandler {
	return &(LoggingEventHandler{
		logger: logger,
	})
}

func (h *LoggingEventHandler) Handle(ctx context.Context, message kafka.Message) error {
	h.logger.Printf(
		"Processing event: topic=%s partition=%d offset=%d key=%s value=%s",
        message.Topic,
        message.Partition,
        message.Offset,
        string(message.Key),
        string(message.Value),
	)

	return nil
}