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
	CWService *services.CloudWatchService
	Logger *logging.StructuredLogger
}

func NewFilesHandler(service *services.DynamoDBService, cwService *services.CloudWatchService) *FilesHandler {
	return &FilesHandler{
		Service: service,
		CWService: cwService,
		Logger: logging.NewStructuredLogger(),
	}
}

func (h *FilesHandler) GetAllFiles(context *gin.Context) {
	start := time.Now()
	traceId := uuid.NewString()
	log := h.Logger.WithTrace(traceId, "api", "GET", "/files")

	data, err := h.Service.GetAllItems(context)

	if err != nil {
		h.CWService.EmitAsyncFailure(context, "GET /files", log)
		log.Error("Failed to retrieve file metadata.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": "Failed to retrieve file metadata.", "detail": err.Error(),})
		return
	}

	latency := time.Since(start).Milliseconds()
	log.Info("All file metadata retrieved successfully",
    	"latency_ms", latency,
	)
	
	h.CWService.EmitAsyncMetrics(context, "GET /files", int(latency), log)

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
		h.CWService.EmitAsyncFailure(context, "GET /files/:id", log)
		log.Error("Failed to retrieve file metadata.", "error", err)
		context.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Failed to retrieve file metadata %s.", fileId), "detail": err.Error(),})
		return
	}

	latency := time.Since(start).Milliseconds()
	log.Info("File metadata retrieved successfully",
    	"latency_ms", time.Since(start).Milliseconds(),
	)	
	h.CWService.EmitAsyncMetrics(context, "GET /files/:id", int(latency), log)
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
		h.CWService.EmitAsyncFailure(context, "GET /files/:id", log)
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
	latency := time.Since(start).Milliseconds()
	log.Info("File processing completed.",
    	"latency_ms", latency,
	)	
	h.CWService.EmitAsyncMetrics(context, "GET /files/:id/status", int(latency), log)
    context.JSON(http.StatusOK, gin.H{
        "status": file.ProcessingState,
        "result": "File processing completed.",
    })
} 