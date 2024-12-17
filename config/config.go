package config

import (
	"log"
	"os"
)

type Config struct {
	HTTPHost       string
	HTTPPort       string
	DBHost         string
	DBPort         string
	DBUser         string
	DBPassword     string
	DBName         string
	KafkaBrokers   string
	KafkaTopic     string
	KafkaGroupID   string
	MigrationsPath string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		HTTPHost:       getEnv("HTTP_HOST", "0.0.0.0"),
		HTTPPort:       getEnv("HTTP_PORT", "8080"),
		DBHost:         getEnv("DB_HOST", "localhost"),
		DBPort:         getEnv("DB_PORT", "5432"),
		DBUser:         getEnv("DB_USER", "postgres"),
		DBPassword:     getEnv("DB_PASSWORD", "1"),
		DBName:         getEnv("DB_NAME", "orders_db"),
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "orders"),
		KafkaGroupID:   getEnv("KAFKA_GROUP_ID", "orders_consumer_group"),
		MigrationsPath: getEnv("MIGRATIONS_PATH", "db/migrations"),
	}

	return cfg, nil
}

func getEnv(key, defaultVal string) string {
	val, exists := os.LookupEnv(key)
	if !exists {
		log.Printf("Переменная в config.env %s не задана, используется значение по умолчанию: %s", key, defaultVal)
		return defaultVal
	}
	return val
}
