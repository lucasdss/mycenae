package limiter

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func New(limit int, burst int, log *zap.Logger) (*RateLimit, error) {
	if limit == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: limit less than 1. %d", limit))
	}

	if burst == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: burst less than 1. %d", burst))
	}

	return &RateLimit{
		max:     burst,
		count:   0,
		limiter: rate.NewLimiter(rate.Limit(limit), burst),

		gblog: log,
	}, nil

}

type RateLimit struct {
	max     int
	count   int
	limiter *rate.Limiter

	gblog *zap.Logger
}

func (rt *RateLimit) Reserve() gobol.Error {

	reservation := rt.limiter.Reserve()
	if !reservation.OK() {
		rt.gblog.Info("Reserve not allowed, burst number")

		return rt.error()
	}
	wait := reservation.Delay()
	if wait == time.Duration(0) {
		return nil
	}

	if rt.count >= rt.max {
		reservation.Cancel()
		rt.gblog.Info("Reserve not allowed, max event at same time")
		return rt.error()
	}
	rt.count++

	rt.gblog.Debug(fmt.Sprintf("waiting %s for event", wait.String()))
	time.Sleep(wait)

	rt.count--

	return nil
}

func (rt *RateLimit) error() gobol.Error {
	return tserr.New(
		fmt.Errorf("too many events"),
		"too many events",
		http.StatusTooManyRequests,
		map[string]interface{}{
			"limiter": "reserve",
		},
	)
}
