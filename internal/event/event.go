package event

type CreateEventRequest struct {
	EventID string `json:"event_id"`
	Type string `json:"type"`
	Payload map[string]any `json:"payload"`
}

type CreateEventResponse struct {
	EventID string `json:"event_id"`
	Type    string `json:"type"`
	Status  string `json:"status"`
}