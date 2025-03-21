package cache

// import (
// 	"runtime"
// 	"sync"
// 	"weak"
// )

// type Cache[K comparable, V any] struct {
// 	cache map[K]weak.Pointer[V]
// 	mu    sync.RWMutex
// }

// func NewCache[K comparable, V any]() *Cache[K, V] {
// 	return &Cache[K, V]{
// 		cache: make(map[K]weak.Pointer[V]),
// 	}
// }

// func (c *Cache[K, V]) Set(key K, value V) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	c.cache[key] = weak.Make(&value)
// 	runtime.AddCleanup(&value, func(key K) {
// 		c.mu.Lock()
// 		defer c.mu.Unlock()
// 		delete(c.cache, key)
// 	}, key)
// }

// func (c *Cache[K, V]) Get(key K) (v V, found bool) {
// 	c.mu.RLock()
// 	defer c.mu.RUnlock()

// 	if value, ok := c.cache[key]; ok {
// 		if item := value.Value(); item != nil {
// 			return *item, true
// 		}
// 	}
// 	return
// }
