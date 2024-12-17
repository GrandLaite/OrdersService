package cache

import (
	"sync"
	"time"

	"order-app/internal/domain"
)

type OrderCache struct {
	data map[string]*cacheEntry
	mu   sync.RWMutex
	ttl  time.Duration
}

type cacheEntry struct {
	order     *domain.Order
	timestamp time.Time
}

func NewCache(ttl time.Duration) *OrderCache {
	c := &OrderCache{
		data: make(map[string]*cacheEntry),
		ttl:  ttl,
	}
	go c.startCleaner()
	return c
}

func (c *OrderCache) Set(order *domain.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[order.OrderUID] = &cacheEntry{
		order:     order,
		timestamp: time.Now(),
	}
}

func (c *OrderCache) Get(orderUID string) (*domain.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.data[orderUID]
	if !exists {
		return nil, false
	}

	if time.Since(entry.timestamp) > c.ttl {
		return nil, false
	}

	return entry.order, true
}

func (c *OrderCache) startCleaner() {
	ticker := time.NewTicker(time.Hour)
	for range ticker.C {
		c.cleanUp()
	}
}

func (c *OrderCache) cleanUp() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for uid, entry := range c.data {
		if now.Sub(entry.timestamp) > c.ttl {
			delete(c.data, uid)
		}
	}
}
