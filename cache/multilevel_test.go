package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

type demoEntity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (d *demoEntity) Identity() string { return d.ID }

func TestGetLoadsFromSourceAndCaches(t *testing.T) {
	client := newFakeRedis()
	loadCount := 0
	cache := NewMultiLevelCache(func(e Entity) error {
		loadCount++
		ent := e.(*demoEntity)
		ent.Name = "from-source"
		return nil
	}, WithRedis(client))

	entity := &demoEntity{ID: "1"}
	if err := cache.Get(entity); err != nil {
		t.Fatalf("unexpected error on first get: %v", err)
	}
	if entity.Name != "from-source" {
		t.Fatalf("expected populated entity, got %q", entity.Name)
	}

	entity2 := &demoEntity{ID: "1"}
	if err := cache.Get(entity2); err != nil {
		t.Fatalf("unexpected error on second get: %v", err)
	}
	if loadCount != 1 {
		t.Fatalf("expected loader to run once, got %d", loadCount)
	}
}

func TestRedisHitAvoidsSource(t *testing.T) {
	client := newFakeRedis()
	primingCache := NewMultiLevelCache(func(e Entity) error {
		ent := e.(*demoEntity)
		ent.Name = "from-source"
		return nil
	}, WithRedis(client))

	seed := &demoEntity{ID: "2"}
	if err := primingCache.Get(seed); err != nil {
		t.Fatalf("unexpected error priming cache: %v", err)
	}

	loadCount := 0
	cache := NewMultiLevelCache(func(Entity) error {
		loadCount++
		return nil
	}, WithRedis(client))

	entity := &demoEntity{ID: "2"}
	if err := cache.Get(entity); err != nil {
		t.Fatalf("unexpected error on redis hit: %v", err)
	}
	if loadCount != 0 {
		t.Fatalf("loader should not run when redis hits, got %d", loadCount)
	}
	if entity.Name != "from-source" {
		t.Fatalf("expected value from redis, got %q", entity.Name)
	}
}

func TestDeleteEvictsBothLevels(t *testing.T) {
	client := newFakeRedis()
	cache := NewMultiLevelCache(func(e Entity) error {
		ent := e.(*demoEntity)
		ent.Name = "to-delete"
		return nil
	}, WithRedis(client), WithExpiration(time.Minute))

	entity := &demoEntity{ID: "3"}
	if err := cache.Get(entity); err != nil {
		t.Fatalf("unexpected error populating cache: %v", err)
	}

	if err := cache.Delete(entity); err != nil {
		t.Fatalf("unexpected error deleting: %v", err)
	}

	if _, ok := cache.l1[cache.cacheKey(entity)]; ok {
		t.Fatalf("expected L1 entry to be removed")
	}

	if client.Exists(cache.cacheKey(entity)) {
		t.Fatalf("expected redis key to be removed")
	}
}

func TestExpirationExpiresL1Entry(t *testing.T) {
	loadCount := 0
	cache := NewMultiLevelCache(func(e Entity) error {
		loadCount++
		ent := e.(*demoEntity)
		ent.Name = "expiring"
		return nil
	}, WithExpiration(5*time.Millisecond))

	entity := &demoEntity{ID: "4"}
	if err := cache.Get(entity); err != nil {
		t.Fatalf("unexpected error on first load: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	entity2 := &demoEntity{ID: "4"}
	if err := cache.Get(entity2); err != nil {
		t.Fatalf("unexpected error after expiration: %v", err)
	}

	if loadCount != 2 {
		t.Fatalf("expected loader to run twice due to expiration, got %d", loadCount)
	}
}

type fakeRedis struct {
	mu    sync.Mutex
	data  map[string]fakeRedisEntry
	nowFn func() time.Time
}

type fakeRedisEntry struct {
	value     []byte
	expiresAt time.Time
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: make(map[string]fakeRedisEntry), nowFn: time.Now}
}

func (f *fakeRedis) Get(key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.data[key]
	if !ok || (!item.expiresAt.IsZero() && f.nowFn().After(item.expiresAt)) {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(string(item.value), nil)
}

func (f *fakeRedis) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = append([]byte(nil), v...)
	case string:
		bytes = []byte(v)
	default:
		bytes = []byte(fmt.Sprint(v))
	}

	var expires time.Time
	if expiration > 0 {
		expires = f.nowFn().Add(expiration)
	}

	f.data[key] = fakeRedisEntry{value: bytes, expiresAt: expires}
	return redis.NewStatusResult("OK", nil)
}

func (f *fakeRedis) Del(keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()

	var removed int64
	for _, key := range keys {
		if _, ok := f.data[key]; ok {
			delete(f.data, key)
			removed++
		}
	}

	return redis.NewIntResult(removed, nil)
}

func (f *fakeRedis) Exists(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	item, ok := f.data[key]
	if !ok {
		return false
	}
	return item.expiresAt.IsZero() || f.nowFn().Before(item.expiresAt)
}
