// Loads environment variables and creates configuration

package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Bucket string
	TableName string
}

func LoadConfig() *Config {
	err := godotenv.Load()

	if err != nil {
		log.Fatalf("Could not retrieve environment variables: %w.", err)
	}

	bucket := os.Getenv("BUCKET_NAME")
	tableName := os.Getenv("TABLE_NAME")

	if bucket == "" {
		log.Fatal("Could not retrieve Bucket.")
	}

	if tableName == "" {
		log.Fatal("Could not retrieve TableName.")
	}

	return &Config{
		Bucket:     bucket,
		TableName: 	tableName,
	}
}