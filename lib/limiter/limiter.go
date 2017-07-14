package limiter

import (
	"errors"
	"fmt"
	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"net/http"
	"time"
)

func New(limit int, burst int, log *zap.Logger) (*RateLimite, error) {
	if limit == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: limit less then 1. %d", limit))
	}

	if burst == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: burst less then 1. %d", burst))
	}

	l := rate.Limit(limit)

	return &RateLimite{
		max:     burst,
		count:   0,
		limiter: rate.NewLimiter(l, burst),

		gblog: log,
	}, nil

}

type RateLimite struct {
	max     int
	count   int
	limiter *rate.Limiter

	gblog *zap.Logger
}

func (rt *RateLimite) Reserve() gobol.Error {

	reservation := rt.limiter.Reserve()
	if !reservation.OK() {
		rt.gblog.Info("Reserve not allowed, burst number")

		return rt.error()
	}
	wait := reservation.Delay()
	if wait == time.Second*0 {
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

func (rt *RateLimite) error() gobol.Error {
	return tserr.New(
		fmt.Errorf("too many events"),
		"too many evebts",
		http.StatusTooManyRequests,
		map[string]interface{}{
			"limiter": "reserve",
		},
	)
}
