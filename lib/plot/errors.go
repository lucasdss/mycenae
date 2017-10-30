package plot

import (
	"errors"
	"fmt"
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
			"package": "plot",
			"func":    "New",
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
				"package": "plot",
				"func":    f,
			},
		)
	}
	return nil
}

func errPointsOverflow(function string, received, max int) gobol.Error {
	m := fmt.Sprintf("points received %v overflow max %v", received, max)
	return errBasic(function, m, http.StatusRequestEntityTooLarge, errors.New(m))
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

func errParamSize(f string, e error) gobol.Error {
	return errBasic(f, `query param "size" should be an integer number greater than zero`, http.StatusBadRequest, e)
}

func errParamFrom(f string, e error) gobol.Error {
	return errBasic(f, `query param "from" should be an integer number greater or equals zero`, http.StatusBadRequest, e)
}

func errPersist(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusInternalServerError, e)
}

func errValidationE(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusBadRequest, e)
}

func errEmptyExpression(f string) gobol.Error {
	return errBasic(f, "no expression found", http.StatusBadRequest, errors.New("no expression found"))
}
