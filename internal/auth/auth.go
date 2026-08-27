package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/wahyudi-hh/HookFlow/internal/api"
	"github.com/wahyudi-hh/HookFlow/internal/client"
)

type ClientRespository interface {
	FindByAPIKeyDigest(ctx context.Context, apiKeyDigest string) (*client.Client, error)
}

type clientIDKey struct{}

type Authenticator struct {
	clientRepo 	ClientRespository
	appSecret 	string
}

func ClientID(ctx context.Context) (uuid.UUID, bool) {
	clientID, ok := ctx.Value(clientIDKey{}).(uuid.UUID)
	return clientID, ok
}

func NewAuthenticator(clientRepo ClientRespository, appSecret string) *Authenticator {
	return &Authenticator{clientRepo: clientRepo, appSecret: appSecret}
}

func HashAPIKey(apiKey string, secretKey string) string {
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(apiKey))
	return hex.EncodeToString(mac.Sum(nil))
}

func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			api.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing API key")
			return
		}

		apiKeyHash := HashAPIKey(apiKey, a.appSecret)
		client, err := a.clientRepo.FindByAPIKeyDigest(r.Context(), apiKeyHash)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				api.WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid API key")
				return
			}
			api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "An internal error occurred")
			return
		}
		ctx := context.WithValue(r.Context(), clientIDKey{}, client.ID)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}