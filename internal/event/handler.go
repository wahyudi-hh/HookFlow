package event

import (
	"encoding/json"
	"net/http"
)

func CreateEventHandler(w http.ResponseWriter, r *http.Request) {
	var request CreateEventRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.EventID == "" || request.Type == "" {
		http.Error(w, "Missing event_id or type fields", http.StatusBadRequest)
		return
	}

	response := CreateEventResponse{
		EventID: request.EventID,
		Type:    request.Type,
		Status:  "ACCEPTED",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}