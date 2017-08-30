package udpError

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
)

func (uerror *UDPerror) ListErrorTags(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/errortags", "keyspace": "empty"})
		rip.Fail(w, errNotFound("ListErrorTags"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/errortags", "keyspace": ks})

	query := StructV2Error{}

	gerr := rip.FromJSON(r, &query)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	q := r.URL.Query()

	sizeStr := q.Get("size")

	var size int

	if sizeStr != "" {

		size, err := strconv.Atoi(sizeStr)
		if err != nil {
			gerr := errParamSize("ListErrorTags", err)
			rip.Fail(w, gerr)
			return
		}
		if size <= 0 {
			gerr := errParamSize("ListErrorTags", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	fromStr := q.Get("from")

	var from int

	if fromStr != "" {

		from, err := strconv.Atoi(fromStr)
		if err != nil {
			gerr := errParamFrom("ListErrorTags", err)
			rip.Fail(w, gerr)
			return
		}
		if from < 0 {
			gerr := errParamFrom("ListErrorTags", errors.New(""))
			rip.Fail(w, gerr)
			return
		}
	}

	keys, total, gerr := uerror.listErrorTags(
		ks,
		"errortag",
		query.Metric,
		query.Tags,
		int64(size),
		int64(from),
	)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(keys) == 0 {
		gerr := errNoContent("ListErrorTags")
		rip.Fail(w, gerr)
		return
	}

	out := Response{
		TotalRecords: total,
		Payload:      keys,
	}

	rip.SuccessJSON(w, http.StatusOK, out)
	return
}

func (uerror *UDPerror) GetErrorInfo(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	ks := ps.ByName("keyspace")
	if ks == "" {
		rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/errors/#error", "keyspace": "empty"})
		rip.Fail(w, errNotFound("GetErrorInfo"))
		return
	}

	rip.AddStatsMap(r, map[string]string{"path": "/keyspaces/#keyspace/errors/#error", "keyspace": ks})

	errID := ps.ByName("error")
	if errID == "" {
		rip.Fail(w, errNotFound("GetErrorInfo"))
		return
	}

	errorList, gerr := uerror.getErrorInfo(ks, errID)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}
	if len(errorList) == 0 {
		gerr := errNoContent("GetErrorInfo")
		rip.Fail(w, gerr)
		return
	}

	rip.SuccessJSON(w, http.StatusOK, errorList)
	return
}
