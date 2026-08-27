package event

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

type Handler struct {
	service EventService
}

func NewHandler(service EventService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var request EventRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.EventID == "" || request.Type == "" {
		http.Error(w, "Missing event_id or type fields", http.StatusBadRequest)
		return
	}

	clientID := uuid.MustParse(
		"00000000-0000-0000-0000-000000000001",
	)

	createdEvent, err := h.service.CreateEvent(r.Context(), clientID, request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := EventResponse{
		EventID: createdEvent.ClientEventId,
		Type:    createdEvent.Type,
		Status:  "ACCEPTED",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}