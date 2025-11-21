package cache

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// GormFetcher matches freedom's Repository/Infra FetchOnlyDB method, allowing
// MultiLevelCache to backfill data directly from the configured gorm.DB source.
type GormFetcher interface {
	FetchOnlyDB(db interface{}) error
}

// WithGormLoader configures the cache to load missing entities from the database
// using gorm. The fetcher should be a freedom Repository/Infra implementation
// that can populate a *gorm.DB via FetchOnlyDB. Optionally, an identity column
// can be provided; otherwise gorm's primary key lookup is used.
func WithGormLoader(fetcher GormFetcher, identityColumn ...string) Option {
	var column string
	if len(identityColumn) > 0 {
		column = identityColumn[0]
	}

	return func(c *MultiLevelCache) {
		c.loader = func(e Entity) error {
			var db *gorm.DB
			if err := fetcher.FetchOnlyDB(&db); err != nil {
				return err
			}
			if db == nil {
				return errors.New("cache: gorm db not found")
			}

			var tx *gorm.DB
			if column == "" {
				tx = db.First(e, e.Identity())
			} else {
				tx = db.Where(fmt.Sprintf("%s = ?", column), e.Identity()).First(e)
			}
			return tx.Error
		}
	}
}
