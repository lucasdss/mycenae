package limiter

import (
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/tserr"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	logger *zap.Logger
)

func New(limit int64, burst int, log *zap.Logger) (*RateLimit, error) {
	if limit == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: limit less than 1. %d", limit))
	}

	if burst == 0 {
		return nil, errors.New(fmt.Sprintf("Limiter: burst less than 1. %d", burst))
	}

	logger = log.With(
		zap.String("package", "limiter"),
		zap.String("struct", "RateLimit"),
	)

	return &RateLimit{
		limit:   limit,
		max:     int64(burst) + limit,
		limiter: rate.NewLimiter(rate.Limit(limit), burst),
	}, nil

}

type RateLimit struct {
	limit   int64
	max     int64
	count   int64
	limiter *rate.Limiter

	log *zap.Logger
}

func (rt *RateLimit) Reserve() gobol.Error {

	atomic.AddInt64(&rt.count, 1)
	defer atomic.AddInt64(&rt.count, -1)

	reservation := rt.limiter.Reserve()
	defer reservation.Cancel()
	if !reservation.OK() {
		return rt.error(
			"reserve not allowed, burst number",
			"Reserve",
		)
	}

	wait := reservation.Delay()
	if wait == time.Duration(0) {
		return nil
	}

	c := atomic.LoadInt64(&rt.count)
	if c >= rt.max {
		return rt.error(
			"reserve not allowed, max event at same time",
			"Reserve",
		)
	}

	logger.Debug(
		"waiting for event",
		zap.Duration("wait_time", wait),
	)
	time.Sleep(wait)

	return nil
}

func (rt *RateLimit) error(msg, function string) gobol.Error {
	c := atomic.LoadInt64(&rt.count)
	return tserr.New(
		fmt.Errorf(msg),
		msg,
		http.StatusTooManyRequests,
		map[string]interface{}{
			"limiter": "reserve",
			"func":    function,
			"burst":   rt.max,
			"count":   c,
			"limit":   rt.limit,
		},
	)
}
