package db

import (
	"fmt"
	"log"

	"order-app/config"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func RunMigrations(cfg *config.Config) error {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName)

	m, err := migrate.New(
		"file://"+cfg.MigrationsPath,
		dsn,
	)
	if err != nil {
		return fmt.Errorf("не удалось создать миграцию: %w", err)
	}

	if err = m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("не удалось выполнить миграции: %w", err)
	}

	log.Println("Миграции успешно выполнены.")
	return nil
}
