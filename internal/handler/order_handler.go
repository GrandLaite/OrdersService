package handler

import (
	"net/http"

	"order-app/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type OrderHandler struct {
	svc    *service.OrderService
	logger *zap.Logger
}

func NewOrderHandler(svc *service.OrderService, logger *zap.Logger) *OrderHandler {
	return &OrderHandler{
		svc:    svc,
		logger: logger,
	}
}

func (h *OrderHandler) GetOrderByID(c *gin.Context) {
	id := c.Param("id")

	order, err := h.svc.GetOrder(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("Не удалось получить заказ", zap.String("order_id", id), zap.Error(err))
		c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
		return
	}

	c.JSON(http.StatusOK, order)
}
