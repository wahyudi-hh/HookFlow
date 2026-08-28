package event

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/wahyudi-hh/HookFlow/internal/api"
	"github.com/wahyudi-hh/HookFlow/internal/auth"
)

type mockEventService struct{
	err error
}

func (m *mockEventService) CreateEvent(ctx context.Context, clientID uuid.UUID, request EventRequest) (*Event, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &Event{
		ID:            uuid.New(),
		ClientID:      clientID,
		ClientEventId: request.EventID,
		Type:          request.Type,
		Payload:       request.Payload,
	}, nil
}

func newTestHandler() *Handler {
	service := &mockEventService{}
	return NewHandler(service)
}

func addClientID(req *http.Request) *http.Request {
	clientID := uuid.New()

	ctx := auth.WithClientID(req.Context(), clientID)

	return req.WithContext(ctx)
}

func TestCreateEventHandler(t *testing.T) {
	body := `{"event_id": "123", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	req = addClientID(req)
	response := httptest.NewRecorder()

	handler := newTestHandler()
	handler.CreateEvent(response, req)

	if response.Code != http.StatusAccepted {
		t.Fatalf("Expected status code %d, got %d", http.StatusAccepted, response.Code)
	}
}

func TestCreateEventHandler_InvalidJSON(t *testing.T) {
	body := `{"event_id": }`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler := newTestHandler()
	handler.CreateEvent(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}

	var responseBody api.ErrorResponse
	err := json.NewDecoder(response.Body).Decode(&responseBody)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if responseBody.Error.Code != "INVALID_REQUEST" {
		t.Fatalf("Expected error code %s, got %s", "INVALID_REQUEST", responseBody.Error.Code)
	}
}

func TestCreateEventHandler_MissingEventID(t *testing.T) {
	body := `{"event_id": "", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler := newTestHandler()
	handler.CreateEvent(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}

	var responseBody api.ErrorResponse
	err := json.NewDecoder(response.Body).Decode(&responseBody)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if responseBody.Error.Code != "MISSING_FIELDS" {
		t.Fatalf("Expected error code %s, got %s", "MISSING_FIELDS", responseBody.Error.Code)
	}
}

func TestCreateEvent_MissingType(t *testing.T) {
	body := `{"event_id": "123", "type": "", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler := newTestHandler()
	handler.CreateEvent(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}

	var responseBody api.ErrorResponse
	err := json.NewDecoder(response.Body).Decode(&responseBody)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if responseBody.Error.Code != "MISSING_FIELDS" {
		t.Fatalf("Expected error code %s, got %s", "MISSING_FIELDS", responseBody.Error.Code)
	}
}

func TestCreateEventHandler_DuplicateEvent(t *testing.T) {
	service := &mockEventService{err: ErrorDuplicateEvent}
	handler := NewHandler(service)
	body := `{"event_id": "123", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	req = addClientID(req)
	response := httptest.NewRecorder()

	handler.CreateEvent(response, req)

	if response.Code != http.StatusConflict {
		t.Fatalf("Expected status code %d, got %d", http.StatusConflict, response.Code)
	}

	var responseBody api.ErrorResponse
	err := json.NewDecoder(response.Body).Decode(&responseBody)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if responseBody.Error.Code != "EVENT_ALREADY_EXISTS" {
		t.Fatalf("Expected error code %s, got %s", "EVENT_ALREADY_EXISTS", responseBody.Error.Code)
	}
}



func TestCreateEventHandler_InternalError(t *testing.T) {
	service := &mockEventService{err: errors.New("internal error")}
	handler := NewHandler(service)
	body := `{"event_id": "123", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	req = addClientID(req)
	response := httptest.NewRecorder()

	handler.CreateEvent(response, req)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("Expected status code %d, got %d", http.StatusInternalServerError, response.Code)
	}
	var responseBody api.ErrorResponse
	err := json.NewDecoder(response.Body).Decode(&responseBody)
	if err != nil {
		t.Fatalf("Failed to decode response body: %v", err)
	}
	if responseBody.Error.Code != "INTERNAL_SERVER_ERROR" {
		t.Fatalf("Expected error code %s, got %s", "INTERNAL_SERVER_ERROR", responseBody.Error.Code)
	}
}