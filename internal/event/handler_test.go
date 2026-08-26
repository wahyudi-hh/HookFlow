package event

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateEventHandler(t *testing.T) {
	body := `{"event_id": "123", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	CreateEventHandler(response, req)

	if response.Code != http.StatusAccepted {
		t.Fatalf("Expected status code %d, got %d", http.StatusAccepted, response.Code)
	}
}

func TestCreateEventHandler_InvalidJSON(t *testing.T) {
	body := `{"event_id": }`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	CreateEventHandler(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}
}

func TestCreateEventHandler_MissingEventID(t *testing.T) {
	body := `{"event_id": "", "type": "order.completed", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	CreateEventHandler(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}
}

func TestCreateEventHandler_MissingType(t *testing.T) {
	body := `{"event_id": "123", "type": "", "payload": {"order_id": "order-123"}}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	response := httptest.NewRecorder()

	CreateEventHandler(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("Expected status code %d, got %d", http.StatusBadRequest, response.Code)
	}
}