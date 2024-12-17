package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"order-app/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

func (r *OrderRepository) SaveOrder(ctx context.Context, order *domain.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("не удалось распарсить заказ: %w", err)
	}

	query := `
		INSERT INTO orders (order_uid, data)
		VALUES ($1, $2)
		ON CONFLICT (order_uid) DO NOTHING;
	`

	_, err = r.pool.Exec(ctx, query, order.OrderUID, data)
	if err != nil {
		return fmt.Errorf("не удалось вставить заказ в БД: %w", err)
	}
	return nil
}

func (r *OrderRepository) GetOrder(ctx context.Context, uid string) (*domain.Order, error) {
	var data []byte
	query := `SELECT data FROM orders WHERE order_uid = $1 LIMIT 1;`
	err := r.pool.QueryRow(ctx, query, uid).Scan(&data)
	if err != nil {
		return nil, fmt.Errorf("заказ не найден или произошла ошибка запроса: %w", err)
	}

	var order domain.Order
	if err = json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("не удалось распарсить заказ: %w", err)
	}

	return &order, nil
}

func (r *OrderRepository) GetAllOrders(ctx context.Context) ([]*domain.Order, error) {
	rows, err := r.pool.Query(ctx, "SELECT data FROM orders;")
	if err != nil {
		return nil, fmt.Errorf("не удалось получить все заказы: %w", err)
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("не удалось считать данные заказа: %w", err)
		}

		var o domain.Order
		if err := json.Unmarshal(data, &o); err != nil {
			return nil, fmt.Errorf("не удалось распарсить заказ: %w", err)
		}
		orders = append(orders, &o)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("ошибка итерации по строкам результата: %w", rows.Err())
	}

	return orders, nil
}
