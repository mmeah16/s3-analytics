package api

import (
	"net/http"
	"s3-analytics/internal/api/handlers"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(server *gin.Engine, uploadHandler *handlers.UploadHandler) {
	server.POST("/files", uploadHandler.UploadFile)
}

func health(context *gin.Context) {
	context.JSON(http.StatusOK, gin.H{
		"message": "Hello World!",
	})
}