package event

import (
	"context"
	"github.com/google/uuid"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

type EventService interface {
	CreateEvent(
		ctx context.Context,
		clientID uuid.UUID,
		request EventRequest,
	) (*Event, error)
}

func (s *Service) CreateEvent(ctx context.Context, clientId uuid.UUID, request EventRequest)(*Event, error) {
	event := Event{
		ID:            uuid.New(),
		ClientID:      clientId,
		ClientEventId: request.EventID,
		Type:          request.Type,
		Payload:       request.Payload,
	}

	err := s.repo.CreateEvent(ctx, event)
	if err != nil {
		return nil, err
	}

	return &event, nil
}