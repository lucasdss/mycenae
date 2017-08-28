package bcache

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tsstats"
)

func newBolt(path string, stats *tsstats.StatsTS) (*persistence, gobol.Error) {
	var err error

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, errPersist("New", err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, errPersist("New", err)
	}
	defer tx.Rollback()

	if _, err = tx.CreateBucketIfNotExists([]byte("keyspace")); err != nil {
		return nil, errPersist("New", err)
	}

	if _, err = tx.CreateBucketIfNotExists([]byte("number")); err != nil {
		return nil, errPersist("New", err)
	}

	if _, err = tx.CreateBucketIfNotExists([]byte("text")); err != nil {
		return nil, errPersist("New", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, errPersist("New", err)
	}

	return &persistence{
		db:    db,
		stats: stats,
	}, nil
}

type persistence struct {
	db    *bolt.DB
	stats *tsstats.StatsTS
}

func (persist *persistence) Get(buckName, key []byte) ([]byte, gobol.Error) {
	start := time.Now()
	tx, err := persist.db.Begin(false)
	if err != nil {
		statsError(persist.stats, "begin", buckName)
		return nil, errPersist("Get", err)
	}

	defer tx.Rollback()
	bucket := tx.Bucket(buckName)

	val := bucket.Get(key)
	if val == nil {
		statsNotFound(persist.stats, buckName)
		return nil, nil
	}
	statsSuccess(persist.stats, "get", buckName, time.Since(start))
	return append([]byte{}, val...), nil
}

func (persist *persistence) Put(buckName, key, value []byte) gobol.Error {
	start := time.Now()
	tx, err := persist.db.Begin(true)
	if err != nil {
		statsError(persist.stats, "begin", buckName)
		return errPersist("Put", err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket(buckName)
	if err = bucket.Put(key, value); err != nil {
		statsError(persist.stats, "put", buckName)
		return errPersist("Put", err)
	}

	err = tx.Commit()
	if err != nil {
		statsError(persist.stats, "put", buckName)
		return errPersist("Put", err)
	}

	statsSuccess(persist.stats, "put", buckName, time.Since(start))
	return nil
}

func (persist *persistence) Delete(buckName, key []byte) gobol.Error {
	start := time.Now()
	tx, err := persist.db.Begin(true)
	if err != nil {
		statsError(persist.stats, "begin", buckName)
		return errPersist("Delete", err)
	}
	defer tx.Rollback()

	bucket := tx.Bucket(buckName)
	if err = bucket.Delete(key); err != nil {
		statsError(persist.stats, "delete", buckName)
		return errPersist("delete", err)
	}

	err = tx.Commit()
	if err != nil {
		statsError(persist.stats, "delete", buckName)
		return errPersist("delete", err)
	}

	statsSuccess(persist.stats, "delete", buckName, time.Since(start))
	return nil
}

type kvPair struct {
	K []byte
	V []byte
}

func (persist *persistence) Load(keyspace []byte) []kvPair {
	var kv []kvPair

	persist.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(keyspace))
		if b != nil {
			b.ForEach(func(k, v []byte) error {
				kv = append(kv, kvPair{
					K: k,
					V: v,
				})
				return nil
			})
		}
		return nil
	})
	return kv
}
