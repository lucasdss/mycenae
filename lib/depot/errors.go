package depot

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errInit(s string) gobol.Error {

	return tserr.New(
		errors.New(s),
		s,
		http.StatusInternalServerError,
		map[string]interface{}{
			"package": "depot",
			"func":    "NewCassandra",
		},
	)
}

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			code,
			map[string]interface{}{
				"package": "depot",
				"func":    f,
			},
		)
	}
	return nil
}

func errValidationS(f, s string) gobol.Error {
	return errBasic(f, s, http.StatusBadRequest, errors.New(s))
}

func errNotFound(f string) gobol.Error {
	return errBasic(f, "", http.StatusNotFound, errors.New(""))
}

func errValidation(f, m string, e error) gobol.Error {
	return errBasic(f, m, http.StatusBadRequest, e)
}

func errNoContent(f string) gobol.Error {
	return errBasic(f, "", http.StatusNoContent, errors.New(""))
}

func errPersist(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusInternalServerError, e)
}
