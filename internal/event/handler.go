package event

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
)

type Handler struct {
	service EventService
}

func NewHandler(service EventService) *Handler {
	return &Handler{service: service}
}

type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	response := ErrorResponse{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	var request EventRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid JSON payload")
		return
	}
	if request.EventID == "" || request.Type == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELDS", "Missing event_id or type fields")
		return
	}

	clientID := uuid.MustParse(
		"00000000-0000-0000-0000-000000000001",
	)

	createdEvent, err := h.service.CreateEvent(r.Context(), clientID, request)
	if err != nil {
		if errors.Is(err, ErrorDuplicateEvent) {
			writeError(w, http.StatusConflict, "EVENT_ALREADY_EXISTS", "Event with the same event_id already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "An internal error occurred")
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