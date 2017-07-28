package tools

import (
	"fmt"
	"log"

	"github.com/gocql/gocql"
)

type keyspaceCassandra struct {
	session *gocql.Session
}

type TableProperties struct {
	Bloom_filter_fp_chance      float64
	Caching                     map[string]string
	Comment                     string
	Compaction                  map[string]string
	Compression                 map[string]string
	Dclocal_read_repair_chance  float64
	Default_time_to_live        int
	Gc_grace_seconds            int
	Max_index_interval          int
	Memtable_flush_period_in_ms int
	Min_index_interval          int
	Read_repair_chance          float64
	Speculative_retry           string
}

type KeyspaceAttributes struct {
	Name                    string
	Replication_factor      int
	Datacenter              string
	Ks_ttl                  int
	Ks_tuuid                bool
	Contact                 string
	Replication_factor_meta string
}

type KeyspaceProperties struct {
	Keyspace_name  string
	Durable_writes bool
}

func (ks *keyspaceCassandra) init(session *gocql.Session) {
	ks.session = session
}

const (
	cqlCountKeyspaces     = `SELECT count(*) FROM system_schema.keyspaces;`
	cqlCountTsKeyspaces   = `SELECT count(*) FROM mycenae.ts_keyspace;`
	cqlKeyspaceTables     = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`
	cqlExists             = `SELECT count(*) FROM system_schema.keyspaces WHERE keyspace_name = ?`
	cqlExistsInformation  = `SELECT count(*) FROM mycenae.ts_keyspace WHERE key = ? and name = ? and replication_factor = ? and datacenter = ? and ks_ttl = ? and ks_tuuid = ? and contact = ? ALLOW FILTERING`
	cqlTableProperties    = `SELECT bloom_filter_fp_chance, caching, comment, compaction, compression, dclocal_read_repair_chance, default_time_to_live, gc_grace_seconds, max_index_interval, memtable_flush_period_in_ms, min_index_interval, read_repair_chance, speculative_retry from system_schema.tables  where keyspace_name = ? and table_name = ?`
	cqlKeyspaceProperties = `SELECT keyspace_name, durable_writes from system_schema.keyspaces where keyspace_name = ?`
	cqlDropKS             = `DROP KEYSPACE %v`
	cqlSelectKS           = `SELECT name, replication_factor, datacenter, ks_ttl, ks_tuuid, contact, replication_factor_meta FROM mycenae.ts_keyspace WHERE key = ?`
	cqlDeleteKS           = `DELETE FROM mycenae.ts_keyspace WHERE key = '%v'`
	cqlInsertKS           = `INSERT INTO mycenae.ts_keyspace (key, name , datacenter , replication_factor, ks_ttl, ks_tuuid) VALUES ('%v', '%v', 'dc_gt_a1', 1, 90, false)`
)

func (ks *keyspaceCassandra) CountKeyspaces() (count int) {
	if err := ks.session.Query(cqlCountKeyspaces).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ks *keyspaceCassandra) CountTsKeyspaces() (count int) {
	if err := ks.session.Query(cqlCountTsKeyspaces).Scan(&count); err != nil {
		log.Println(err)
	}
	return
}

func (ks *keyspaceCassandra) KeyspaceTables(keyspace string) (tables []string) {
	iter := ks.session.Query(
		cqlKeyspaceTables,
		keyspace,
	).Iter()

	var table string

	for iter.Scan(&table) {
		tables = append(tables, table)
	}
	if err := iter.Close(); err != nil {
		log.Println(err)
	}
	return
}

func (ks *keyspaceCassandra) Exists(keyspace string) bool {
	var count int
	err := ks.session.Query(
		cqlExists,
		keyspace,
	).Scan(&count)
	return err == nil && count > 0
}

func (ks *keyspaceCassandra) ExistsInformation(keyspace string, name string, replication_factor int, datacenter string, ttl int, tuuid bool, contact string) bool {
	var count int
	err := ks.session.Query(
		cqlExistsInformation,
		keyspace,
		name,
		replication_factor,
		datacenter,
		ttl,
		tuuid,
		contact,
	).Scan(&count)
	return err == nil && count > 0
}

func (ks *keyspaceCassandra) TableProperties(keyspace string, table string) TableProperties {
	var caching, compaction, compression map[string]string
	var speculative_retry, comment string
	var default_time_to_live, gc_grace_seconds, max_index_interval, memtable_flush_period_in_ms,
		min_index_interval int
	var bloom_filter_fp_chance, dclocal_read_repair_chance, read_repair_chance float64

	if err := ks.session.Query(cqlTableProperties,
		keyspace,
		table,
	).Scan(&bloom_filter_fp_chance, &caching, &comment, &compaction, &compression, &dclocal_read_repair_chance,
		&default_time_to_live, &gc_grace_seconds, &max_index_interval, &memtable_flush_period_in_ms, &min_index_interval,
		&read_repair_chance, &speculative_retry); err != nil {
		log.Println(err)
	}

	return TableProperties{
		Bloom_filter_fp_chance:      bloom_filter_fp_chance,
		Caching:                     caching,
		Comment:                     comment,
		Compaction:                  compaction,
		Compression:                 compression,
		Dclocal_read_repair_chance:  dclocal_read_repair_chance,
		Default_time_to_live:        default_time_to_live,
		Gc_grace_seconds:            gc_grace_seconds,
		Max_index_interval:          max_index_interval,
		Memtable_flush_period_in_ms: memtable_flush_period_in_ms,
		Min_index_interval:          min_index_interval,
		Read_repair_chance:          read_repair_chance,
		Speculative_retry:           speculative_retry,
	}
}

func (ks *keyspaceCassandra) Drop(keyspace string) bool {

	err := ks.session.Query(
		fmt.Sprintf(cqlDropKS, keyspace),
	).Exec()

	return err == nil

}

func (ks *keyspaceCassandra) Delete(keyspace string) bool {

	err := ks.session.Query(
		fmt.Sprintf(cqlDeleteKS, keyspace),
	).Exec()

	return err == nil

}

func (ks *keyspaceCassandra) Insert(keyspace string) bool {

	err := ks.session.Query(
		fmt.Sprintf(cqlInsertKS, keyspace, keyspace),
	).Exec()

	return err == nil

}

func (ks *keyspaceCassandra) KsAttributes(keyspace string) KeyspaceAttributes {
	var name, datacenter, contact, replication_factor_meta string
	var replication_factor, ks_ttl int
	var ks_tuuid bool

	if err := ks.session.Query(cqlSelectKS,
		keyspace,
	).Scan(&name, &replication_factor, &datacenter, &ks_ttl, &ks_tuuid, &contact, &replication_factor_meta); err != nil {
		log.Println(err)
	}
	return KeyspaceAttributes{
		Name:                    name,
		Datacenter:              datacenter,
		Contact:                 contact,
		Replication_factor:      replication_factor,
		Ks_ttl:                  ks_ttl,
		Ks_tuuid:                ks_tuuid,
		Replication_factor_meta: replication_factor_meta,
	}
}

func (ks *keyspaceCassandra) KeyspaceProperties(keyspace string) KeyspaceProperties {
	var keyspace_name string
	var durable_writes bool

	if err := ks.session.Query(cqlKeyspaceProperties,
		keyspace,
	).Scan(&keyspace_name, &durable_writes); err != nil {
		log.Println(err)
	}
	return KeyspaceProperties{
		Keyspace_name:  keyspace_name,
		Durable_writes: durable_writes,
	}
}
