package handlers

import (
	"fmt"
	"net/http"
	"s3-analytics/internal/logging"
	"s3-analytics/internal/services"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type FilesHandler struct {
	Service *services.DynamoDBService
	Logger *logging.StructuredLogger
}

func NewFilesHandler(service *services.DynamoDBService) *FilesHandler {
	return &FilesHandler{
		Service: service,
		Logger: logging.NewStructuredLogger(),
	}
}

func (h *FilesHandler) GetAllFiles(context *gin.Context) {
	start := time.Now()
	traceId := uuid.NewString()
	log := h.Logger.WithTrace(traceId, "api", "GET", "/files")

	data, err := h.Service.GetAllItems(context)

	if err != nil {
		log.Error("Failed to retrieve file metadata.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": "Failed to retrieve file metadata.", "detail": err.Error(),})
		return
	}

	log.Info("All file metadata retrieved successfully",
    	"latency_ms", time.Since(start).Milliseconds(),
	)	
	context.JSON(http.StatusOK, gin.H{
        "data": data,
        "message": "All file metadata retrieved successfully.",
    })
} 

func (h *FilesHandler) GetSingleFile(context *gin.Context) {
	start := time.Now()
	traceId := uuid.NewString()
	log := h.Logger.WithTrace(traceId, "api", "GET", "/files/:id")

	fileId := context.Param("id")

	data, err := h.Service.GetFileById(context, fileId)

	if err != nil {
		log.Error("Failed to retrieve file metadata.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to retrieve file metadata %s.", fileId), "detail": err.Error(),})
		return
	}

	log.Info("File metadata retrieved successfully",
    	"latency_ms", time.Since(start).Milliseconds(),
	)	
	context.JSON(http.StatusOK, gin.H{
        "data": data,
        "message": fmt.Sprintf("File metadata %s retrieved successfully.", fileId),
    })
} 

func (h *FilesHandler) GetFileStatus(context *gin.Context) {
	start := time.Now()
	traceId := uuid.NewString()
	log := h.Logger.WithTrace(traceId, "api", "GET", "/files/:id/status")

	fileId := context.Param("id")

	file, err := h.Service.GetFileById(context, fileId)

	if err != nil {
		log.Error("Failed to retrieve file status.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to retrieve file status %s.", fileId), "detail": err.Error(),})
		return
	}

    if file.ProcessingState != "done" {
		log.Info("File processing not completed yet.")
        context.JSON(http.StatusOK, gin.H{
            "status": file.ProcessingState,
            "result": "processing not completed yet",
        })
        return
    }

	log.Info("File processing completed.",
    	"latency_ms", time.Since(start).Milliseconds(),
	)	
    context.JSON(http.StatusOK, gin.H{
        "status": file.ProcessingState,
        "result": "File processing completed.",
    })
} 