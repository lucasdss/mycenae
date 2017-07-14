package meta

import (
	"io"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/gobol/rubber"
)

type persistence struct {
	esearch *rubber.Elastic
}

func (persist *persistence) HeadMetaFromES(index, eType, id string) (int, gobol.Error) {
	start := time.Now()
	respCode, err := persist.esearch.GetHead(index, eType, id)
	if err != nil {
		statsIndexError(index, eType, "head")
		return 0, errPersist("HeadMetaFromES", err)
	}
	statsIndex(index, eType, "head", time.Since(start))
	return respCode, nil
}

func (persist *persistence) SendErrorToES(index, eType, id string, doc StructV2Error) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.Put(index, eType, id, doc)
	if err != nil {
		statsIndexError(index, eType, "put")
		return errPersist("SendErrorToES", err)
	}
	statsIndex(index, eType, "PUT", time.Since(start))
	return nil
}

func (persist *persistence) SaveBulkES(body io.Reader) gobol.Error {
	start := time.Now()
	_, err := persist.esearch.PostBulk(body)
	if err != nil {
		statsIndexError("", "", "bulk")
		return errPersist("SaveBulkES", err)
	}
	statsIndex("", "", "bulk", time.Since(start))
	return nil
}
