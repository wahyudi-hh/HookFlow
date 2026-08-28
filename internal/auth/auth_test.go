package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/wahyudi-hh/HookFlow/internal/api"
	"github.com/wahyudi-hh/HookFlow/internal/client"
)

type mockClientRepository struct {
	client       *client.Client
	err          error
	receivedHash string
}

func (m *mockClientRepository) FindByAPIKeyDigest(ctx context.Context, apiKeyDigest string) (*client.Client, error) {
	m.receivedHash = apiKeyDigest
	return m.client, m.err
}

func TestHashAPIKey(t *testing.T) {
	got := HashAPIKey("api-key", "app-secret")
	want := "33ff92f6d5a91102fa250b629d345fe9efa5000f7d92f177615310822c117d98"

	if got != want {
		t.Fatalf("HashAPIKey() = %q, want %q", got, want)
	}
}

func TestClientID(t *testing.T) {
	clientID := uuid.New()
	ctx := context.WithValue(context.Background(), clientIDKey{}, clientID)

	got, ok := ClientID(ctx)
	if !ok || got != clientID {
		t.Fatalf("ClientID() = %v, %v; want %v, true", got, ok, clientID)
	}

	if _, ok := ClientID(context.Background()); ok {
		t.Fatal("ClientID() unexpectedly found a client ID")
	}
}

func TestAuthenticatorMiddleware(t *testing.T) {
	clientID := uuid.New()
	secret := "app-secret"

	tests := []struct {
		name       string
		apiKey     string
		repoClient *client.Client
		repoErr    error
		wantStatus int
		wantCode   string
		wantNext   bool
	}{
		{
			name:       "missing API key",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "UNAUTHORIZED",
		},
		{
			name:       "invalid API key",
			apiKey:     "invalid-key",
			repoErr:    pgx.ErrNoRows,
			wantStatus: http.StatusUnauthorized,
			wantCode:   "UNAUTHORIZED",
		},
		{
			name:       "repository error",
			apiKey:     "api-key",
			repoErr:    errors.New("database unavailable"),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "INTERNAL_SERVER_ERROR",
		},
		{
			name:       "valid API key",
			apiKey:     "api-key",
			repoClient: &client.Client{ID: clientID},
			wantStatus: http.StatusNoContent,
			wantNext:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := &mockClientRepository{client: test.repoClient, err: test.repoErr}
			nextCalled := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				gotID, ok := ClientID(r.Context())
				if !ok || gotID != clientID {
					t.Errorf("ClientID() = %v, %v; want %v, true", gotID, ok, clientID)
				}
				w.WriteHeader(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/protected", nil)
			if test.apiKey != "" {
				req.Header.Set("X-API-Key", test.apiKey)
			}
			response := httptest.NewRecorder()

			NewAuthenticator(repo, secret).Middleware(next).ServeHTTP(response, req)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			if nextCalled != test.wantNext {
				t.Fatalf("next called = %v, want %v", nextCalled, test.wantNext)
			}
			if test.wantCode != "" {
				var body api.ErrorResponse
				if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
					t.Fatalf("decode error response: %v", err)
				}
				if body.Error.Code != test.wantCode {
					t.Fatalf("error code = %q, want %q", body.Error.Code, test.wantCode)
				}
			}
			if test.wantNext && repo.receivedHash != HashAPIKey(test.apiKey, secret) {
				t.Fatalf("repository hash = %q, want HMAC digest", repo.receivedHash)
			}
		})
	}
}
