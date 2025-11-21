package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Entity represents the minimal interface that can be cached.
// The value returned by Identity is used to build the cache key.
type Entity interface {
	Identity() string
}

// Serializer is responsible for converting entities to and from bytes.
type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONSerializer is the default serializer using the encoding/json package.
type JSONSerializer struct{}

func (JSONSerializer) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONSerializer) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Option configures MultiLevelCache behavior.
type Option func(*MultiLevelCache)

// WithPrefix sets the key prefix used for all cached entities.
func WithPrefix(prefix string) Option {
	return func(c *MultiLevelCache) {
		c.prefix = prefix
	}
}

// WithExpiration configures the TTL for values written to Redis and the L1 cache.
func WithExpiration(expiration time.Duration) Option {
	return func(c *MultiLevelCache) {
		c.expiration = expiration
	}
}

// WithAsyncWrite toggles asynchronous writes to Redis for cache fills and deletions.
func WithAsyncWrite(async bool) Option {
	return func(c *MultiLevelCache) {
		c.asyncWrite = async
	}
}

// WithRedis assigns the Redis client used for the second-level cache.
// Passing nil disables Redis entirely.
func WithRedis(client redisClient) Option {
	return func(c *MultiLevelCache) {
		c.redis = client
	}
}

// WithSerializer customizes the serializer used by the cache.
func WithSerializer(serializer Serializer) Option {
	return func(c *MultiLevelCache) {
		c.serializer = serializer
	}
}

// MultiLevelCache provides a framework-free implementation of a two-level cache.
// L1 is a process-local in-memory map with TTL support. L2 is Redis.
type MultiLevelCache struct {
	mu         sync.RWMutex
	l1         map[string]entry
	loader     func(Entity) error
	prefix     string
	expiration time.Duration
	asyncWrite bool
	serializer Serializer
	redis      redisClient
	now        func() time.Time
}

type redisClient interface {
	Get(key string) *redis.StringCmd
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(keys ...string) *redis.IntCmd
}

type entry struct {
	payload   []byte
	expiresAt time.Time
}

// NewMultiLevelCache builds a new cache instance. A loader must be provided to
// hydrate entities when both caches miss.
func NewMultiLevelCache(loader func(Entity) error, opts ...Option) *MultiLevelCache {
	cache := &MultiLevelCache{
		l1:         make(map[string]entry),
		loader:     loader,
		prefix:     "ec",
		expiration: 5 * time.Minute,
		serializer: JSONSerializer{},
		asyncWrite: false,
		now:        time.Now,
	}

	for _, opt := range opts {
		opt(cache)
	}

	return cache
}

// Get populates the given entity using the multi-level cache strategy.
func (c *MultiLevelCache) Get(entity Entity) error {
	key := c.cacheKey(entity)

	if ok, err := c.getFromL1(key, entity); ok || err != nil {
		return err
	}

	if data, err := c.getFromRedis(key); err == nil {
		c.setL1(key, data)
		return c.serializer.Unmarshal(data, entity)
	} else if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	if c.loader == nil {
		return errors.New("cache: undefined source loader")
	}

	if err := c.loader(entity); err != nil {
		return err
	}

	data, err := c.serializer.Marshal(entity)
	if err != nil {
		return err
	}

	c.setL1(key, data)
	return c.writeRedis(key, data)
}

// Delete evicts the entity from both cache layers. If async is provided and true,
// the Redis deletion runs in a goroutine.
func (c *MultiLevelCache) Delete(entity Entity, async ...bool) error {
	key := c.cacheKey(entity)
	c.deleteL1(key)

	if c.redis == nil {
		return nil
	}

	doAsync := len(async) > 0 && async[0]
	if !doAsync {
		return c.redis.Del(key).Err()
	}

	go c.safeAsync(func() error { return c.redis.Del(key).Err() })
	return nil
}

func (c *MultiLevelCache) cacheKey(entity Entity) string {
	name := reflect.TypeOf(entity)
	for name.Kind() == reflect.Ptr {
		name = name.Elem()
	}
	if c.prefix == "" {
		return fmt.Sprintf("%s:%s", name.Name(), entity.Identity())
	}
	return fmt.Sprintf("%s:%s:%s", c.prefix, name.Name(), entity.Identity())
}

func (c *MultiLevelCache) getFromL1(key string, entity Entity) (bool, error) {
	c.mu.RLock()
	e, ok := c.l1[key]
	c.mu.RUnlock()
	if !ok {
		return false, nil
	}
	if !e.expiresAt.IsZero() && c.now().After(e.expiresAt) {
		c.deleteL1(key)
		return false, nil
	}
	return true, c.serializer.Unmarshal(e.payload, entity)
}

func (c *MultiLevelCache) setL1(key string, data []byte) {
	var expires time.Time
	if c.expiration > 0 {
		expires = c.now().Add(c.expiration)
	}

	c.mu.Lock()
	c.l1[key] = entry{payload: data, expiresAt: expires}
	c.mu.Unlock()
}

func (c *MultiLevelCache) deleteL1(key string) {
	c.mu.Lock()
	delete(c.l1, key)
	c.mu.Unlock()
}

func (c *MultiLevelCache) getFromRedis(key string) ([]byte, error) {
	if c.redis == nil {
		return nil, redis.Nil
	}
	return c.redis.Get(key).Bytes()
}

func (c *MultiLevelCache) writeRedis(key string, data []byte) error {
	if c.redis == nil {
		return nil
	}
	if !c.asyncWrite {
		return c.redis.Set(key, data, c.expiration).Err()
	}

	go c.safeAsync(func() error { return c.redis.Set(key, data, c.expiration).Err() })
	return nil
}

func (c *MultiLevelCache) safeAsync(fn func() error) {
	var err error
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
		if err != nil {
			// best-effort logging without framework
			fmt.Printf("cache async error: %v\n", err)
		}
	}()
	err = fn()
}
