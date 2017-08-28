package collector

import (
	"fmt"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/meta"
)

func (collector *Collector) saveText(packet gorilla.Point) gobol.Error {
	return collector.persist.InsertText(
		packet.KsID,
		packet.ID,
		packet.Timestamp,
		packet.Message.Text,
	)
}

func (collector *Collector) saveError(
	recvTags map[string]string,
	metric,
	keyspace,
	esIndex,
	id,
	msg,
	errMsg string,
) gobol.Error {

	now := time.Now()
	ks := keyspace
	if keyspace == collector.settings.Depot.Cassandra.Keyspace {
		ks = "default"
	}
	statsUDPerror(ks, "number")

	idks := fmt.Sprintf("%s%s", id, keyspace)
	gerr := collector.persist.InsertError(idks, msg, errMsg, now)
	if gerr != nil {
		return gerr
	}

	var tags []meta.Tag
	for k, v := range recvTags {
		tags = append(tags, meta.Tag{
			Key:   k,
			Value: v,
		})
	}
	collector.persist.SendErrorToES(esIndex, "errortag", id, meta.ErrorData{
		Key:    id,
		Metric: metric,
		Tags:   tags,
	})
	return nil
}
