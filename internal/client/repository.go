package client

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) FindByAPIKeyDigest(ctx context.Context, apiKeyDigest string) (*Client, error) {
	var client Client
	err := r.db.QueryRow(ctx, "SELECT id, name, api_key_digest FROM clients WHERE api_key_digest = $1", apiKeyDigest).
		Scan(&client.ID, &client.Name, &client.APIKeyDigest)
	if err != nil {
		return nil, err
	}
	return &client, nil
}	
