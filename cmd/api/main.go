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
	ctx := context.Background()

	s3Client := aws.NewS3Client(ctx, config.Bucket)
	s3Service := services.NewS3Service(s3Client)

	dynamoDBClient := aws.NewDynamoDBClient(ctx, config.TableName)
	dynamoDBService := services.NewDynamoDBService(dynamoDBClient)

	uploadHandler := handlers.NewUploadHandler(s3Service, dynamoDBService)
	filesHander := handlers.NewFilesHandler(dynamoDBService)

	server := gin.Default()
	api.RegisterRoutes(server, uploadHandler, filesHander)
	server.Run(":8080")
}