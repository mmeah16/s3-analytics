package handlers

import (
	"net/http"
	"s3-analytics/internal/services"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type UploadHandler struct {
	S3Service *services.S3Service
	DynamoDBService *services.DynamoDBService
}

func NewUploadHandler(s3Service *services.S3Service, dynamoDBService *services.DynamoDBService) *UploadHandler {
	return &UploadHandler{
		S3Service: s3Service,
		DynamoDBService: dynamoDBService,
	}
}

func (h *UploadHandler) UploadFile(context *gin.Context) {
	file, err := context.FormFile("file")

	if err != nil {
		context.JSON(http.StatusBadRequest,gin.H{"error": "Upload failed.", "detail": err.Error(),})
		return
	}

	key, err := h.S3Service.UploadFileToS3(context, file)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Upload failed.", "detail": err.Error(),})
		return
	}
	uuid := uuid.New().String()
	// Create file metadata and put item into DynamoDB
	metadata := services.FileMetadata{
		ID:        		 uuid,
		Filename:  		 file.Filename,
		Size:      		 file.Size,
		ProcessingState: "uploaded",
		CreatedAt: 		 time.Now().UTC(),
	}

	_, err = h.DynamoDBService.CreateItem(context, &metadata)
	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Metadata record creation failed.", "detail": err.Error(),})
		return
	}

    context.JSON(http.StatusOK, gin.H{
        "key": key,
        "message": "Upload successful.",
    })

}