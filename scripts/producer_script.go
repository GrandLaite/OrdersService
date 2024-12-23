package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type Order struct {
	OrderUID          string       `json:"order_uid"`
	TrackNumber       int          `json:"track_number"`
	Entry             string       `json:"entry"`
	Delivery          DeliveryInfo `json:"delivery"`
	Payment           PaymentInfo  `json:"payment"`
	Items             []Item       `json:"items"`
	Locale            string       `json:"locale"`
	InternalSignature string       `json:"internal_signature"`
	CustomerID        string       `json:"customer_id"`
	DeliveryService   string       `json:"delivery_service"`
	Shardkey          string       `json:"shardkey"`
	SmID              int          `json:"sm_id"`
	DateCreated       time.Time    `json:"date_created"`
	OofShard          string       `json:"oof_shard"`
}

type DeliveryInfo struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type PaymentInfo struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

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

	order := Order{
		OrderUID:    gofakeit.LetterN(15),
		TrackNumber: gofakeit.Number(1, 1000),
		Entry:       gofakeit.RandomString([]string{"WBIL", "ECOM", "STORE"}),
		Delivery: DeliveryInfo{
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: PaymentInfo{
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
		Items: []Item{
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
