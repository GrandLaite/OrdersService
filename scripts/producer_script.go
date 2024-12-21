package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"order-app/internal/domain"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	if err := godotenv.Load("config.env"); err != nil {
		log.Println("Файл config.env не найден. Будут использованы значения по умолчанию.")
	}

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9093"
	}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "orders"
	}

	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBrokers),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gofakeit.Seed(time.Now().UnixNano())

	order := domain.Order{
		OrderUID:    gofakeit.LetterN(15),
		TrackNumber: gofakeit.Number(1, 1000),
		Entry:       gofakeit.RandomString([]string{"WBIL", "ECOM", "STORE"}),
		Delivery: domain.DeliveryInfo{
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: domain.PaymentInfo{
			Transaction:  gofakeit.UUID(),
			RequestID:    "",
			Currency:     "USD",
			Provider:     gofakeit.Company(),
			Amount:       gofakeit.Number(100, 2000),
			PaymentDt:    time.Now().Unix(),
			Bank:         gofakeit.Company(),
			DeliveryCost: gofakeit.Number(100, 500),
			GoodsTotal:   gofakeit.Number(50, 1000),
			CustomFee:    gofakeit.Number(0, 100),
		},
		Items: []domain.Item{
			{
				ChrtID:      gofakeit.Number(100000, 999999),
				TrackNumber: gofakeit.Regex("WBILMTESTTRACK[0-9]{4}"),
				Price:       gofakeit.Number(100, 1000),
				Rid:         gofakeit.UUID(),
				Name:        gofakeit.ProductName(),
				Sale:        gofakeit.Number(0, 50),
				Size:        gofakeit.RandomString([]string{"S", "M", "L"}),
				TotalPrice:  gofakeit.Number(100, 2000),
				NmID:        gofakeit.Number(100000, 999999),
				Brand:       gofakeit.Company(),
				Status:      gofakeit.Number(100, 300),
			},
		},
		Locale:            gofakeit.RandomString([]string{"en", "ru", "fr"}),
		InternalSignature: "",
		CustomerID:        gofakeit.Username(),
		DeliveryService:   gofakeit.RandomString([]string{"meest", "dhl", "ups"}),
		Shardkey:          gofakeit.DigitN(1),
		SmID:              gofakeit.Number(1, 999),
		DateCreated:       time.Now().UTC(),
		OofShard:          gofakeit.DigitN(1),
	}

	data, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("Не удалось сериализовать данные заказа: %v", err)
	}

	err = w.WriteMessages(ctx, kafka.Message{
		Value: data,
	})
	if err != nil {
		log.Fatalf("Не удалось отправить сообщение в Kafka: %v", err)
	}

	log.Println("Заказ успешно отправлен в Kafka:", order.OrderUID)
}
