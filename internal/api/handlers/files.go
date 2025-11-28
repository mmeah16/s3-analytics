package handlers

import (
	"fmt"
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

func (h *FilesHandler) GetAllFiles(context *gin.Context) {

	data, err := h.Service.GetAllItems(context)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Failed to retrieve file metadata.", "detail": err.Error(),})
		return
	}

	context.JSON(http.StatusOK, gin.H{
        "data": data,
        "message": "File metadata retrieved successfully.",
    })
} 

func (h *FilesHandler) GetSingleFile(context *gin.Context) {
	
	fileId := context.Param("id")

	data, err := h.Service.GetFileById(context, fileId)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to retrieve file metadata %s.", fileId), "detail": err.Error(),})
		return
	}

	context.JSON(http.StatusOK, gin.H{
        "data": data,
        "message": fmt.Sprintf("File metadata %s retrieved successfully.", fileId),
    })
} 

func (h *FilesHandler) GetFileStatus(context *gin.Context) {
	
	fileId := context.Param("id")

	file, err := h.Service.GetFileById(context, fileId)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to retrieve file metadata %s.", fileId), "detail": err.Error(),})
		return
	}

    if file.ProcessingState != "done" {
        context.JSON(http.StatusOK, gin.H{
            "status": file.ProcessingState,
            "result": "processing not completed yet",
        })
        return
    }

    context.JSON(http.StatusOK, gin.H{
        "status": file.ProcessingState,
        "result": "processed output would appear here",
    })
} 