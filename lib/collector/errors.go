package collector

import (
	"errors"
	"net/http"

	"golang.org/x/time/rate"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errBR(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusBadRequest,
			map[string]interface{}{
				"package": "collector",
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
				"package": "collector",
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

func errRateLimit(function string, limit rate.Limit, burst int) gobol.Error {
	return tserr.New(
		errors.New("too many requests"),
		"too many requests",
		http.StatusTooManyRequests,
		map[string]interface{}{
			"func":  function,
			"burst": burst,
			"limit": limit,
		},
	)
}
