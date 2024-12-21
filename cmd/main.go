package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"order-app/config"
	"order-app/db"
	"order-app/internal/cache"
	"order-app/internal/handler"
	"order-app/internal/kafka"
	"order-app/internal/logger"
	"order-app/internal/repository"
	"order-app/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

func main() {
	if err := godotenv.Load("config.env"); err != nil {
		log.Println("Файл config.env не найден. Будут использованы значения по умолчанию.")
	}

	zapLogger, err := logger.NewZapLogger()
	if err != nil {
		log.Fatalf("Ошибка инициализации логгера: %v", err)
	}
	defer zapLogger.Sync()

	cfg, err := config.LoadConfig()
	if err != nil {
		zapLogger.Fatal("Не удалось загрузить конфигурацию", zap.Error(err))
	}

	if err := db.RunMigrations(cfg); err != nil {
		zapLogger.Fatal("Не удалось использовать миграции", zap.Error(err))
	}

	pool, err := config.InitDB(cfg)
	if err != nil {
		zapLogger.Fatal("Не удалось подключиться к базе данных", zap.Error(err))
	}
	defer pool.Close()

	cacheStorage := cache.NewCache(24 * time.Hour)

	repo := repository.NewOrderRepository(pool)

	svc := service.NewOrderService(repo, cacheStorage)

	if loadedCount, err := svc.RestoreCache(); err != nil {
		zapLogger.Error("Не удалось загрузить данные из БД в кэш", zap.Error(err))
	} else {
		zapLogger.Info("Успешно загружены данные из БД в кэш", zap.Int("количество заказов", loadedCount))
	}

	consumer, err := kafka.NewConsumer(cfg, svc, zapLogger)
	if err != nil {
		zapLogger.Fatal("Не удалось создать consumer для Kafka", zap.Error(err))
	}
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	go consumer.Start(consumerCtx)

	r := gin.Default()
	handler.RegisterRoutes(r, svc, zapLogger)

	httpServer := &http.Server{
		Addr:    cfg.HTTPHost + ":" + cfg.HTTPPort,
		Handler: r,
	}

	go func() {
		zapLogger.Info("Запуск HTTP сервера", zap.String("адрес", httpServer.Addr))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			zapLogger.Fatal("Ошибка HTTP сервера", zap.Error(err))
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	sig := <-stopChan
	zapLogger.Info("Получен сигнал для завершения работы", zap.String("сигнал", sig.String()))

	consumerCancel()
	consumer.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		zapLogger.Error("Не удалось корректно завершить работу HTTP сервера", zap.Error(err))
	}

	zapLogger.Info("Сервис успешно завершил работу")
}
