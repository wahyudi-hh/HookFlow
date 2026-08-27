package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type EventRequest struct {
	EventID string         	`json:"event_id"`
	Type    string         	`json:"type"`
	Payload json.RawMessage	`json:"payload"`
}

type EventResponse struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Status  string `json:"status"`
}

type Event struct {
	ID            uuid.UUID
	ClientID      uuid.UUID
	ClientEventId string
	Type          string
	Payload       []byte
	CreatedAt     time.Time
}