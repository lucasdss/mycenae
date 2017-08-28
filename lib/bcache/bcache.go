package bcache

import (
	"sync"

	"github.com/uol/gobol"

	lru "github.com/golang/groupcache/lru"
	"github.com/uol/mycenae/lib/tsstats"
)

//New creates a struct that "caches" timeseries keys. It uses boltdb as persistence
func New(sts *tsstats.StatsTS, path string) (*Bcache, gobol.Error) {
	persist, gerr := newBolt(path, sts)
	if gerr != nil {
		return nil, gerr
	}

	b := &Bcache{
		persist: persist,
		tsmap:   lru.New(2000000),
		ksmap:   lru.New(256),
	}

	go b.load()

	return b, nil

}

//Bcache is responsible for caching timeseries keys from elasticsearch
type Bcache struct {
	persist *persistence
	tsmap   *lru.Cache
	ksmap   *lru.Cache
	ksmtx   sync.Mutex
	tsmtx   sync.Mutex
}

func (bc *Bcache) load() {
	bc.tsmtx.Lock()
	defer bc.tsmtx.Unlock()

	for _, kv := range bc.persist.Load([]byte("number")) {
		//bc.tsmap[string(kv.K)] = nil
		bc.tsmap.Add(string(kv.K), nil)
	}
}

// GetTsNumber checks if a numeric timeseries is cached
func (bc *Bcache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("meta", "number", key, CheckTSID)
}

// GetTsText checks if a text timeseries is cached
func (bc *Bcache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("metatext", "text", key, CheckTSID)
}

// Get checks the LRU cache
func (bc *Bcache) Get(ksts []byte) bool {

	bc.tsmtx.Lock()
	_, ok := bc.tsmap.Get(string(ksts))
	bc.tsmtx.Unlock()

	return ok
}

func (bc *Bcache) Set(key string) gobol.Error {
	gerr := bc.persist.Put([]byte("number"), []byte(key), []byte{})
	if gerr != nil {
		return gerr
	}

	bc.tsmtx.Lock()
	bc.tsmap.Add(key, nil)
	bc.tsmtx.Unlock()
	return nil
}

func (bc *Bcache) getTSID(esType, bucket, key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	bc.tsmtx.Lock()
	_, ok := bc.tsmap.Get(key)
	bc.tsmtx.Unlock()

	if ok {
		return true, nil
	}

	go func() {
		v, gerr := bc.persist.Get([]byte(bucket), []byte(key))
		if gerr != nil {
			return
		}
		if v != nil {
			bc.tsmtx.Lock()
			bc.tsmap.Add(key, nil)
			bc.tsmtx.Unlock()
			return
		}

		found, gerr := CheckTSID(esType, key)
		if gerr != nil {
			return
		}
		if !found {
			return
		}

		gerr = bc.persist.Put([]byte(bucket), []byte(key), []byte{})
		if gerr != nil {
			return
		}

		bc.tsmtx.Lock()
		bc.tsmap.Add(key, nil)
		bc.tsmtx.Unlock()
		return
	}()
	return false, nil
}
