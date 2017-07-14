package udpError

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"

	"go.uber.org/zap"
)

type persistence struct {
	cassandra     *gocql.Session
	esearch       *rubber.Elastic
	consistencies []gocql.Consistency
}

func (persist *persistence) GetErrorInfo(key string) ([]ErrorInfo, gobol.Error) {
	start := time.Now()

	var id, errorMsg, payload string
	date := time.Time{}
	errorsInfo := []ErrorInfo{}
	var err error

	for _, cons := range persist.consistencies {

		iter := persist.cassandra.Query(
			`SELECT tsid, error, message, date FROM ts_error WHERE tsid = ? ALLOW FILTERING`,
			key,
		).Consistency(cons).RoutingKey([]byte(key)).Iter()

		for iter.Scan(&id, &errorMsg, &payload, &date) {
			ei := ErrorInfo{
				ID:      id,
				Error:   errorMsg,
				Message: payload,
				Date:    date,
			}
			errorsInfo = append(errorsInfo, ei)
		}

		if err := iter.Close(); err != nil {

			gblog.Error(
				"",
				zap.String("package", "udpError/persistence"),
				zap.String("func", "GetErrorInfo"),
				zap.Error(err),
			)

			if err == gocql.ErrNotFound {
				statsSelct("default", "ts_error", time.Since(start))
				return []ErrorInfo{}, errNotFound("GetErrorInfo")
			}
			statsSelectQerror("default", "ts_error")
			continue
		}
		statsSelct("default", "ts_error", time.Since(start))
		return errorsInfo, nil
	}
	statsSelectFerror("default", "ts_error")
	return []ErrorInfo{}, errPersist("GetErrorInfo", err)
}

func (persist *persistence) ListESErrorTags(
	esIndex,
	esType string,
	esQuery interface{},
	response *EsResponseTag,
) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.Query(esIndex, esType, esQuery, response)
	if err != nil {
		statsIndexError(esIndex, esType, "POST")
		return errPersist("ListESErrorTags", err)
	}
	statsIndex(esIndex, esType, "POST", time.Since(start))
	return nil
}
