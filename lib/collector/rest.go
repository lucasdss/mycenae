package collector

import (
	"errors"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/uol/gobol/rip"
	"github.com/uol/mycenae/lib/gorilla"
)

func (collect *Collector) Scollector(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	points := gorilla.TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(points) < 1 {
		msg := "must send at least one point"
		rip.Fail(w, errBR("Scollector", msg, errors.New(msg)))
		return
	}

	returnPoints, gerr := collect.HandlePoint(points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) Text(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	rl := collect.wLimiter.Reserve()
	if !rl.OK() {
		rip.Fail(
			w,
			errRateLimit(
				"Text",
				collect.wLimiter.Limit(),
				collect.wLimiter.Burst(),
			),
		)
		return
	}
	time.Sleep(rl.Delay())

	points := gorilla.TSDBpoints{}

	gerr := rip.FromJSON(r, &points)
	if gerr != nil {
		rip.Fail(w, gerr)
		return
	}

	returnPoints := RestErrors{}

	restChan := make(chan RestError, len(points))

	for _, point := range points {
		collect.concPoints <- struct{}{}
		go collect.handleRESTpacket(point, restChan)
	}

	var reqKS string
	var numKS int

	for range points {

		re := <-restChan

		ks := "invalid"
		if collect.isKSIDValid(re.Datapoint.Tags["ksid"]) {
			ks = re.Datapoint.Tags["ksid"]
		}

		if ks != reqKS {
			reqKS = ks
			numKS++
		}

		if re.Gerr != nil {

			statsPointsError(ks, "text")

			reu := RestErrorUser{
				Datapoint: re.Datapoint,
				Error:     re.Gerr.Message(),
			}

			returnPoints.Errors = append(returnPoints.Errors, reu)

		} else {

			statsPoints(ks, "text")

		}
	}

	if len(returnPoints.Errors) > 0 {

		returnPoints.Failed = len(returnPoints.Errors)
		returnPoints.Success = len(points) - len(returnPoints.Errors)

		rip.SuccessJSON(w, http.StatusBadRequest, returnPoints)
		return
	}

	rip.Success(w, http.StatusNoContent, nil)
	return
}

func (collect *Collector) handleRESTpacket(rcvMsg gorilla.TSDBpoint, restChan chan RestError) {
	recvPoint := rcvMsg

	rcvMsg.Value = nil

	restChan <- RestError{
		Datapoint: recvPoint,
		Gerr:      collect.HandleTxtPacket(rcvMsg),
	}

	<-collect.concPoints
}
