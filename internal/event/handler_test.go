package event

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type mockEventService struct{}

func (m *mockEventService) CreateEvent(ctx context.Context, clientID uuid.UUID, request EventRequest) (*Event, error) {
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

func TestCreateEventHandler(t *testing.T) {
	body := `{"event_id": "123", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
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
}