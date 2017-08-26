package keyspace

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	maxTTL   int
	validKey *regexp.Regexp
)

// DefaultCompaction defines the default compaction strategy that cassandra
// will use for timeseries data
const DefaultCompaction = "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy"

// New creates a keyspace storage
func New(
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	meta *meta.Meta,
	usernameGrant,
	keyspaceMain string,
	compaction string,
	mTTL int,
) *Keyspace {

	maxTTL = mTTL
	validKey = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z_]+$`)

	if compaction == "" {
		compaction = DefaultCompaction
	}

	keyspace := &Keyspace{
		persist: persistence{
			cassandra:     cass,
			usernameGrant: usernameGrant,
			keyspaceMain:  keyspaceMain,
			compaction:    compaction,
			stats:         sts,
			meta:          meta,
		},
		cache: make(map[string]bool),
	}

	go keyspace.warmUpCache()
	return keyspace
}

// Keyspace is the manager for the keyspace creation
type Keyspace struct {
	persist persistence

	cache map[string]bool
	sync.Mutex
}

func (keyspace *Keyspace) createKeyspace(ksc Config) (string, gobol.Error) {

	count, gerr := keyspace.persist.countKeyspaceByName(ksc.Name)
	if gerr != nil {
		return "", gerr
	}
	if count != 0 {
		return "", errConflict(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because keyspace "%s" already exists`, ksc.Name),
		)
	}

	count, gerr = keyspace.persist.countDatacenterByName(ksc.Datacenter)
	if gerr != nil {
		return "", gerr
	}
	if count == 0 {
		return "", errValidationS(
			"CreateKeyspace",
			fmt.Sprintf(`Cannot create because datacenter "%s" not exists`, ksc.Datacenter),
		)
	}

	key := generateKey()

	gerr = keyspace.persist.createKeyspace(ksc, key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	gerr = keyspace.createIndex(key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		gerr2 = keyspace.deleteIndex(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	gerr = keyspace.persist.createKeyspaceMeta(ksc, key)
	if gerr != nil {
		gerr2 := keyspace.persist.dropKeyspace(key)
		if gerr2 != nil {

		}
		gerr2 = keyspace.deleteIndex(key)
		if gerr2 != nil {

		}
		return key, gerr
	}

	return key, nil
}

func (keyspace *Keyspace) updateKeyspace(ksc ConfigUpdate, key string) gobol.Error {
	count, gerr := keyspace.persist.countKeyspaceByKey(key)
	if gerr != nil {
		return gerr
	}
	if count == 0 {
		return errNotFound("UpdateKeyspace")

	}

	count, gerr = keyspace.persist.countKeyspaceByName(ksc.Name)
	if gerr != nil {
		return gerr
	}
	if count != 0 {
		k, gerr := keyspace.persist.getKeyspaceKeyByName(ksc.Name)
		if gerr != nil {
			return gerr
		}

		if k != key {
			return errConflict(
				"UpdateKeyspace",
				fmt.Sprintf(`Cannot update because keyspace "%s" already exists`, ksc.Name),
			)
		}
	}

	return keyspace.persist.updateKeyspace(ksc, key)
}

func (keyspace *Keyspace) listAllKeyspaces() ([]Config, int, gobol.Error) {
	ks, err := keyspace.persist.listAllKeyspaces()
	return ks, len(ks), err
}

func (keyspace *Keyspace) checkKeyspace(key string) gobol.Error {
	return keyspace.persist.checkKeyspace(key)
}

func generateKey() string {
	return "ts_" + strings.Replace(uuid.New(), "-", "_", 4)
}

func (keyspace *Keyspace) createIndex(esIndex string) gobol.Error {
	return keyspace.persist.createIndex(esIndex)
}

func (keyspace *Keyspace) deleteIndex(esIndex string) gobol.Error {
	return keyspace.persist.deleteIndex(esIndex)
}

// GetKeyspace returns the configuration of a keyspace
func (keyspace *Keyspace) GetKeyspace(key string) (Config, bool, gobol.Error) {
	return keyspace.persist.getKeyspace(key)
}

func (keyspace *Keyspace) getCache(ks string) bool {
	keyspace.Lock()
	defer keyspace.Unlock()
	_, found := keyspace.cache[ks]
	return found
}

func (keyspace *Keyspace) setCache(ks string) {
	keyspace.Lock()
	defer keyspace.Unlock()
	keyspace.cache[ks] = true
}

func (keyspace *Keyspace) warmUpCache() {
	configs, _, err := keyspace.listAllKeyspaces()
	if err != nil {
		return
	}

	keyspace.Lock()
	defer keyspace.Unlock()

	for _, config := range configs {
		keyspace.cache[config.Key] = true
	}
}

// KeyspaceExists checks whether a keyspace exists
func (keyspace *Keyspace) KeyspaceExists(ks string) (bool, gobol.Error) {
	var err gobol.Error
	found := keyspace.getCache(ks)

	if !found {
		_, found, err = keyspace.GetKeyspace(ks)
		if err != nil {
			return false, err
		}
		if found {
			keyspace.setCache(ks)
		}
	}
	return found, nil
}

func (keyspace *Keyspace) listDatacenters() ([]string, gobol.Error) {
	return keyspace.persist.listDatacenters()
}
