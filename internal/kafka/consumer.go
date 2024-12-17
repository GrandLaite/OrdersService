package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"order-app/config"
	"order-app/internal/domain"
	"order-app/internal/service"

	"github.com/segmentio/kafka-go"
	"github.com/xeipuuv/gojsonschema"
	"go.uber.org/zap"
)

type Consumer struct {
	r        *kafka.Reader
	svc      *service.OrderService
	logger   *zap.Logger
	schema   *gojsonschema.Schema
	stopChan chan struct{}
}

func NewConsumer(cfg *config.Config, svc *service.OrderService, log *zap.Logger) (*Consumer, error) {
	schemaLoader := gojsonschema.NewStringLoader(orderSchemaJSON)
	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return nil, fmt.Errorf("не удалось загрузить JSON-схему: %w", err)
	}

	c := &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{cfg.KafkaBrokers},
			Topic:    cfg.KafkaTopic,
			GroupID:  cfg.KafkaGroupID,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		svc:      svc,
		logger:   log,
		schema:   schema,
		stopChan: make(chan struct{}),
	}
	return c, nil
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer context canceled")
			return
		case <-c.stopChan:
			c.logger.Info("Получен сигнал остановки для consumer")
			return
		default:
			m, err := c.r.ReadMessage(ctx)
			if err != nil {
				c.logger.Error("Ошибка при чтении сообщения из Kafka", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			order, err := c.validateAndParse(m.Value)
			if err != nil {
				c.logger.Error("Получено некорректное сообщение", zap.Error(err))
				continue
			}

			if err := c.svc.ProcessOrder(ctx, order); err != nil {
				c.logger.Error("Не удалось обработать заказ", zap.Error(err))
			} else {
				c.logger.Info("Заказ успешно обработан", zap.String("order_uid", order.OrderUID))
			}
		}
	}
}

func (c *Consumer) Stop() {
	close(c.stopChan)
	c.r.Close()
}

func (c *Consumer) validateAndParse(data []byte) (*domain.Order, error) {
	result, err := c.schema.Validate(gojsonschema.NewBytesLoader(data))
	if err != nil {
		return nil, fmt.Errorf("не удалось валидировать JSON: %w", err)
	}
	if !result.Valid() {
		return nil, fmt.Errorf("JSON не соответствует схеме: %v", result.Errors())
	}

	var order domain.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("не удалось распарсить заказ: %w", err)
	}

	return &order, nil
}

const orderSchemaJSON = `
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["order_uid", "track_number", "entry", "delivery", "payment", "items", "locale", "customer_id", "delivery_service", "shardkey", "sm_id", "date_created", "oof_shard"],
  "properties": {
    "order_uid": {"type": "string"},
    "track_number": {"type": "string"},
    "entry": {"type": "string"},
    "delivery": {
      "type": "object",
      "required": ["name", "phone", "zip", "city", "address", "region", "email"],
      "properties": {
        "name": {"type": "string"},
        "phone": {"type": "string"},
        "zip": {"type": "string"},
        "city": {"type": "string"},
        "address": {"type": "string"},
        "region": {"type": "string"},
        "email": {"type": "string", "format": "email"}
      }
    },
    "payment": {
      "type": "object",
      "required": ["transaction", "currency", "provider", "amount", "payment_dt", "bank", "delivery_cost", "goods_total", "custom_fee"],
      "properties": {
        "transaction": {"type": "string"},
        "request_id": {"type": "string"},
        "currency": {"type": "string"},
        "provider": {"type": "string"},
        "amount": {"type": "integer"},
        "payment_dt": {"type": "integer"},
        "bank": {"type": "string"},
        "delivery_cost": {"type": "integer"},
        "goods_total": {"type": "integer"},
        "custom_fee": {"type": "integer"}
      }
    },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["chrt_id", "track_number", "price", "rid", "name", "sale", "size", "total_price", "nm_id", "brand", "status"],
        "properties": {
          "chrt_id": {"type": "integer"},
          "track_number": {"type": "string"},
          "price": {"type": "integer"},
          "rid": {"type": "string"},
          "name": {"type": "string"},
          "sale": {"type": "integer"},
          "size": {"type": "string"},
          "total_price": {"type": "integer"},
          "nm_id": {"type": "integer"},
          "brand": {"type": "string"},
          "status": {"type": "integer"}
        }
      }
    },
    "locale": {"type": "string"},
    "internal_signature": {"type": "string"},
    "customer_id": {"type": "string"},
    "delivery_service": {"type": "string"},
    "shardkey": {"type": "string"},
    "sm_id": {"type": "integer"},
    "date_created": {"type": "string", "format": "date-time"},
    "oof_shard": {"type": "string"}
  }
}
`
