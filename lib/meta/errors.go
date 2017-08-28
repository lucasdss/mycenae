package meta

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

const packageName = "meta"

func errBR(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusBadRequest,
			map[string]interface{}{
				"package": packageName,
				"func":    f,
			},
		)
	}
	return nil
}

func errISE(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusInternalServerError,
			map[string]interface{}{
				"package": packageName,
				"func":    f,
			},
		)
	}
	return nil
}

func errValidation(s string) gobol.Error {
	return errBR("makePacket", s, errors.New(s))
}

func errUnmarshal(f string, e error) gobol.Error {
	return errBR(f, "Wrong JSON format", e)
}

func errMarshal(f string, e error) gobol.Error {
	return errISE(f, e.Error(), e)
}

func errPersist(f string, e error) gobol.Error {
	return errISE(f, e.Error(), e)
}

func errNotImplemented(fname, structure string) gobol.Error {
	return errISE(fname, structure, errors.New("Not implemented error"))
}
