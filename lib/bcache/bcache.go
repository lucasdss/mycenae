package bcache

import (
	"net/http"
	"sync"

	"github.com/uol/gobol"

	lru "github.com/golang/groupcache/lru"
	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	stats *tsstats.StatsTS
)

//New creates a struct that "caches" timeseries keys. It uses boltdb as persistence
func New(sts *tsstats.StatsTS, ks *keyspace.Keyspace, path string) (*Bcache, gobol.Error) {

	stats = sts

	persist, gerr := newBolt(path)
	if gerr != nil {
		return nil, gerr
	}

	b := &Bcache{
		kspace:  ks,
		persist: persist,
		tsmap:   lru.New(2000000),
		ksmap:   lru.New(256),
	}

	go b.load()

	return b, nil

}

//Bcache is responsible for caching timeseries keys from elasticsearch
type Bcache struct {
	kspace  *keyspace.Keyspace
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

//GetKeyspace returns a keyspace key, a boolean that tells whether the key was found or not and an error.
//If the key isn't in boltdb, GetKeyspace tries to fetch the key from cassandra, and if found, puts it in boltdb.
func (bc *Bcache) GetKeyspace(key string) (string, bool, gobol.Error) {

	bc.ksmtx.Lock()
	//_, ok := bc.ksmap[key]
	_, ok := bc.ksmap.Get(key)
	bc.ksmtx.Unlock()

	if ok {
		return string(key), true, nil
	}

	v, gerr := bc.persist.Get([]byte("keyspace"), []byte(key))
	if gerr != nil {
		return "", false, gerr
	}
	if v != nil {
		bc.ksmtx.Lock()
		bc.ksmap.Add(key, nil)
		bc.ksmtx.Unlock()

		return key, true, nil
	}

	_, found, gerr := bc.kspace.GetKeyspace(key)
	if gerr != nil {
		if gerr.StatusCode() == http.StatusNotFound {
			return "", false, nil
		}
		return "", false, gerr
	}
	if !found {
		return "", false, nil
	}

	gerr = bc.persist.Put([]byte("keyspace"), []byte(key), []byte("false"))
	if gerr != nil {
		return "", false, gerr
	}

	bc.ksmtx.Lock()
	bc.ksmap.Add(key, nil)
	bc.ksmtx.Unlock()

	return key, true, nil
}

func (bc *Bcache) GetTsNumber(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {

	return bc.getTSID("meta", "number", key, CheckTSID)
}

func (bc *Bcache) GetTsText(key string, CheckTSID func(esType, id string) (bool, gobol.Error)) (bool, gobol.Error) {
	return bc.getTSID("metatext", "text", key, CheckTSID)
}

func (bc *Bcache) Get(ksts []byte) bool {

	bc.tsmtx.Lock()
	_, ok := bc.tsmap.Get(string(ksts))
	bc.tsmtx.Unlock()

	return ok

}

func (bc *Bcache) Set(key string) {

	gerr := bc.persist.Put([]byte("number"), []byte(key), []byte{})
	if gerr != nil {
		return
	}

	bc.tsmtx.Lock()
	bc.tsmap.Add(key, nil)
	bc.tsmtx.Unlock()

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
