package udpError

import (
	"fmt"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"github.com/uol/mycenae/lib/bcache"
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
	bc *bcache.Bcache,
	es *rubber.Elastic,
	esIndex string,
	consistencies []gocql.Consistency,
) *UDPerror {

	gblog = gbl
	stats = sts

	return &UDPerror{
		persist: persistence{cassandra: cass, esearch: es, consistencies: consistencies},
		boltc:   bc,
		esIndex: esIndex,
	}
}

type UDPerror struct {
	persist persistence
	boltc   *bcache.Bcache
	esIndex string
}

func (error UDPerror) getErrorInfo(keyspace, key string) ([]ErrorInfo, gobol.Error) {

	_, found, gerr := error.boltc.GetKeyspace(keyspace)
	if gerr != nil {
		return nil, gerr
	}

	if !found {
		return nil, errNotFound("GetErrorInfo")
	}

	return error.persist.GetErrorInfo(fmt.Sprintf("%s%s", key, keyspace))
}

func (error UDPerror) listErrorTags(
	keyspace,
	esType,
	metric string,
	tags []Tag,
	size,
	from int64,
) ([]string, int, gobol.Error) {

	_, found, gerr := error.boltc.GetKeyspace(keyspace)
	if gerr != nil {
		return nil, 0, gerr
	}

	if !found {
		return nil, 0, errNotFound("ListErrorTags")
	}

	var esQuery QueryWrapper

	if metric != "" {
		metricTerm := EsRegexp{
			Regexp: map[string]string{
				"metric": metric,
			},
		}
		esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, metricTerm)
	}

	for _, tag := range tags {

		if tag.Key != "" {
			tagKeyTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsError.tagKey": tag.Key,
				},
			}
			esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, tagKeyTerm)
		}

		if tag.Value != "" {
			tagValueTerm := EsRegexp{
				Regexp: map[string]string{
					"tagsError.tagValue": tag.Value,
				},
			}
			esQuery.Query.Bool.Must = append(esQuery.Query.Bool.Must, tagValueTerm)
		}
	}

	if size != 0 {
		esQuery.Size = size
	} else {
		esQuery.Size = 50
	}

	if from != 0 {
		esQuery.From = from
	}

	var esResp EsResponseTag

	gerr = error.persist.ListESErrorTags(keyspace, esType, esQuery, &esResp)

	total := esResp.Hits.Total

	var keys []string

	for _, docs := range esResp.Hits.Hits {

		key := docs.Id

		keys = append(keys, key)

	}

	return keys, total, gerr
}
