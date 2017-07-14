package cluster

import (
	"net/http"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, c int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			c,
			map[string]interface{}{
				"package": "cluster",
				"func":    f,
			},
		)
	}
	return nil
}

func errInit(f string, err error) gobol.Error {
	return errBasic(f, err.Error(), http.StatusInternalServerError, err)
}

func errRequest(f string, c int, err error) gobol.Error {
	return errBasic(f, err.Error(), c, err)
}
