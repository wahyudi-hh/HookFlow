package event

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
}

func NewKafkaConsumer(brokerAddress, topic, groupID string) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic: topic,
		GroupID: groupID,
	})

	return &KafkaConsumer{
		reader: reader,
	}
}

func (c *KafkaConsumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}