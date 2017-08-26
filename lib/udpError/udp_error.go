package udpError

import (
	"fmt"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/keyspace"
	"github.com/uol/mycenae/lib/meta"
	"github.com/uol/mycenae/lib/tsstats"
)

var (
	gblog *zap.Logger
	stats *tsstats.StatsTS
)

func New(
	gbl *zap.Logger,
	sts *tsstats.StatsTS,
	cass *gocql.Session,
	kspace *keyspace.Keyspace,
	meta *meta.Meta,
	esIndex string,
	consistencies []gocql.Consistency,
) *UDPerror {

	gblog = gbl
	stats = sts

	return &UDPerror{
		persist: persistence{cassandra: cass, meta: meta, consistencies: consistencies},
		meta:    meta,
		kspace:  kspace,
		esIndex: esIndex,
	}
}

type UDPerror struct {
	persist persistence
	meta    *meta.Meta
	kspace  *keyspace.Keyspace
	esIndex string
}

func (ue UDPerror) getErrorInfo(keyspace, key string) ([]ErrorInfo, gobol.Error) {
	found, gerr := ue.kspace.KeyspaceExists(keyspace)
	if gerr != nil {
		return nil, gerr
	}

	if !found {
		return nil, errNotFound("GetErrorInfo")
	}

	return ue.persist.GetErrorInfo(fmt.Sprintf("%s%s", key, keyspace))
}
