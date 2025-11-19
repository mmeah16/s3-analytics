package main

import (
	"context"

	"s3-analytics/internal/api"
	"s3-analytics/internal/api/handlers"
	"s3-analytics/internal/aws"
	"s3-analytics/internal/config"
	"s3-analytics/internal/services"

	"github.com/gin-gonic/gin"
)

func main() {

	config := config.LoadConfig()
	context := context.Background()

	s3Client := aws.NewS3Client(context, config.Bucket)
	s3Service := services.NewS3Service(s3Client)

	uploadHandler := handlers.NewUploadHandler(s3Service)

	server := gin.Default()
	api.RegisterRoutes(server, uploadHandler)
	server.Run(":8080")
}