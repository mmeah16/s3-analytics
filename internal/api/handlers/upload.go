package handlers

import (
	"net/http"
	"s3-analytics/internal/logging"
	"s3-analytics/internal/services"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type UploadHandler struct {
	S3Service *services.S3Service
	DynamoDBService *services.DynamoDBService
	Logger *logging.StructuredLogger
}

func NewUploadHandler(s3Service *services.S3Service, dynamoDBService *services.DynamoDBService) *UploadHandler {
	return &UploadHandler{
		S3Service: s3Service,
		DynamoDBService: dynamoDBService,
		Logger: logging.NewStructuredLogger(),
	}
}

func (h *UploadHandler) UploadFile(context *gin.Context) {
	traceId := uuid.NewString()
	log := h.Logger.WithTrace(traceId)
	file, err := context.FormFile("file")

	if err != nil {
		log.Error("Missing file parameter.", "error", err)
		context.JSON(http.StatusBadRequest,gin.H{"error": "Missing file parameter.", "detail": err.Error(),})
		return
	}

	key, id, err := h.S3Service.UploadFileToS3(context, file, traceId)

	if err != nil {
		log.Error("Upload to S3 failed.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": "Upload failed.", "detail": err.Error(),})
		return
	}
	// Create file metadata and put item into DynamoDB
	metadata := services.FileMetadata{
		ID:        		 id,
		Filename:  		 file.Filename,
		Size:      		 file.Size,
		ProcessingState: "uploaded",
		CreatedAt: 		 time.Now().UTC(),
	}

	_, err = h.DynamoDBService.CreateItem(context, &metadata)
	if err != nil {
		log.Error("File metadata record create failed.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": "Metadata record creation failed.", "detail": err.Error(),})
		return
	}

	log.Info("Upload successful.")

    context.JSON(http.StatusOK, gin.H{
        "key": key,
        "message": "Upload successful.",
    })

}