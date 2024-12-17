package handler

import (
	"order-app/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func RegisterRoutes(r *gin.Engine, svc *service.OrderService, logger *zap.Logger) {
	orderHandler := NewOrderHandler(svc, logger)

	r.GET("/order/:id", orderHandler.GetOrderByID)
}
