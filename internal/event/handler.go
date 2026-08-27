package event

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/wahyudi-hh/HookFlow/internal/api"
	"github.com/wahyudi-hh/HookFlow/internal/auth"
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
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON payload")
		return
	}
	if request.EventID == "" || request.Type == "" {
		api.WriteError(w, http.StatusBadRequest, "MISSING_FIELDS", "Missing event_id or type fields")
		return
	}

	clientID, ok := auth.ClientID(r.Context())
	if !ok {
		api.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "client authentication required")
		return
	}

	createdEvent, err := h.service.CreateEvent(r.Context(), clientID, request)
	if err != nil {
		if errors.Is(err, ErrorDuplicateEvent) {
			api.WriteError(w, http.StatusConflict, "EVENT_ALREADY_EXISTS", "Event with the same event_id already exists")
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "An internal error occurred")
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