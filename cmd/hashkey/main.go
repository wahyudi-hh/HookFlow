package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/wahyudi-hh/HookFlow/internal/auth"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env")
	}
	apiKey := os.Getenv("CLIENT_API_KEY") //only for testing purpose, in production, the api key should be generated and stored securely in the database
	appSecret := os.Getenv("APP_SECRET_KEY")
	if apiKey == "" || appSecret == "" {
		log.Fatal("CLIENT_API_KEY and APP_SECRET_KEY are required")
	}

	fmt.Println(auth.HashAPIKey(apiKey, appSecret))
}