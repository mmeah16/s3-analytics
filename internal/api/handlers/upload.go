package handlers

import (
	"net/http"
	"s3-analytics/internal/services"

	"github.com/gin-gonic/gin"
)

type UploadHandler struct {
	Service *services.S3Service
}

func NewUploadHandler(service *services.S3Service) *UploadHandler {
	return &UploadHandler{
		Service: service,
	}
}

func (h *UploadHandler) UploadFile(context *gin.Context) {
	file, err := context.FormFile("file")

	if err != nil {
		context.JSON(http.StatusBadRequest,gin.H{"error": "Upload failed.", "detail": err.Error(),})
		return
	}

	key, err := h.Service.UploadFileToS3(context, file)

	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Upload failed.", "detail": err.Error(),})
		return
	}

    context.JSON(http.StatusOK, gin.H{
        "key": key,
        "message": "Upload successful.",
    })

}