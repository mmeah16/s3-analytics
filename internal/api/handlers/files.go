package handlers

import (
	"net/http"
	"s3-analytics/internal/services"

	"github.com/gin-gonic/gin"
)

type FilesHandler struct {
	Service *services.DynamoDBService
}

func NewFilesHandler(service *services.DynamoDBService) *FilesHandler {
	return &FilesHandler{
		Service: service,
	}
}

func (h *FilesHandler) ReadFilesTable(context *gin.Context) {

	data, err := h.Service.ReadAllItems(context)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Failed to retrieve file metadata.", "detail": err.Error(),})
		return
	}

	context.JSON(http.StatusOK, gin.H{
        "data": data,
        "message": "File metadata retrieved successfully.",
    })
} 