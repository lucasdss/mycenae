package limiter

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
	"golang.org/x/time/rate"
)

func New(limit int64, burst int) (*RateLimit, error) {
	if limit == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: limit less than 1. %d", limit))
	}

	if burst == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: burst less than 1. %d", burst))
	}

	return &RateLimit{
		limiter: rate.NewLimiter(rate.Limit(limit), burst),
	}, nil

}

type RateLimit struct {
	limiter *rate.Limiter
}

func (rt *RateLimit) Reserve() gobol.Error {

	r := rt.limiter.Reserve()

	if !r.OK() {
		return rt.error(
			"reserve not allowed, burst number",
			"Reserve",
			float64(rt.limiter.Limit()),
			rt.limiter.Burst(),
		)
	}

	time.Sleep(r.Delay())

	return nil
}

func (rt *RateLimit) error(msg, function string, limit float64, burst int) gobol.Error {
	return tserr.New(
		fmt.Errorf(msg),
		msg,
		http.StatusTooManyRequests,
		map[string]interface{}{
			"limiter": "reserve",
			"func":    function,
			"burst":   burst,
			"limit":   limit,
		},
	)
}
