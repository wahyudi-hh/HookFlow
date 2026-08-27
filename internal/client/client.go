package client

import (
	"github.com/google/uuid"
)

type Client struct {
	ID 				uuid.UUID
	Name 			string
	APIKeyDigest 	string
}