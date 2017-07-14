package gorilla

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
			"package": "gorilla",
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
				"package": "gorilla",
				"func":    f,
			},
		)
	}
	return nil
}

func errBasicf(f, s string, code int, e error, lf map[string]interface{}) gobol.Error {
	lf["package"] = "gorilla"
	lf["func"] = f
	if e != nil {
		return tserr.New(e, s, code, lf)
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

func errMemoryUpdate(f, msg string) gobol.Error {
	return errBasic(f, msg, http.StatusInternalServerError, errors.New(msg))
}

func errMemoryUpdatef(f, msg string, lf map[string]interface{}) gobol.Error {
	lf["package"] = "gorilla"
	lf["func"] = f
	return errBasic(f, msg, http.StatusInternalServerError, errors.New(msg))
}

func errAddPoint(f string, lf map[string]interface{}) gobol.Error {
	return errBasicf(f, f, http.StatusBadRequest, errors.New(f), lf)
}

func errUpdateDelta(f, ksid, tsid string, blkid, delta int64) gobol.Error {
	e := errors.New("delta out of 2h range")
	return errBasic(
		f,
		fmt.Sprintf("ksid=%v tsid=%v blkid=%v delta=%v - %v", ksid, tsid, blkid, delta, e.Error()),
		http.StatusInternalServerError,
		e,
	)
}

func errTsz(f, ksid, tsid string, blkid int64, e error) gobol.Error {
	return errBasic(
		f,
		fmt.Sprintf("tsz - ksid=%v tsid=%v blkid=%v - %v", ksid, tsid, blkid, e),
		http.StatusInternalServerError,
		e,
	)
}
