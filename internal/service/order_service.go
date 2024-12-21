package service

import (
	"context"
	"fmt"

	"order-app/internal/cache"
	"order-app/internal/domain"
	"order-app/internal/repository"
)

type OrderService struct {
	repo  *repository.OrderRepository
	cache *cache.OrderCache
}

func NewOrderService(repo *repository.OrderRepository, cache *cache.OrderCache) *OrderService {
	return &OrderService{
		repo:  repo,
		cache: cache,
	}
}

func (s *OrderService) RestoreCache() (int, error) {
	ctx := context.Background()
	orders, err := s.repo.GetAllOrders(ctx)
	if err != nil {
		return 0, fmt.Errorf("не удалось восстановить кэш из БД: %w", err)
	}

	for _, o := range orders {
		s.cache.Set(o)
	}

	return len(orders), nil
}

func (s *OrderService) GetOrder(ctx context.Context, orderUID string) (*domain.Order, error) {
	order, found := s.cache.Get(orderUID)
	if found {
		return order, nil
	}

	order, err := s.repo.GetOrder(ctx, orderUID)
	if err != nil {
		return nil, err
	}

	s.cache.Set(order)
	return order, nil
}

func (s *OrderService) ProcessOrder(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return fmt.Errorf("заказ пуст")
	}

	if err := s.repo.SaveOrder(ctx, order); err != nil {
		return err
	}
	s.cache.Set(order)
	return nil
}
