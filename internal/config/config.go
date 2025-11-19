// Loads environment variables and creates configuration

package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Bucket string
}

func LoadConfig() *Config {
	err := godotenv.Load()

	if err != nil {
		log.Fatalf("Could not retrieve environment variables: %w.", err)
	}

	bucket := os.Getenv("BUCKET")

	if bucket == "" {
		log.Fatal("Could not retrieve Bucket.")
	}
	return &Config{
		Bucket:     bucket,
	}
}