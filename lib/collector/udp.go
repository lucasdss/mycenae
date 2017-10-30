package collector

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"
	"golang.org/x/time/rate"

	"go.uber.org/zap"
)

func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {

	atomic.AddInt64(&collector.saving, 1)
	defer atomic.AddInt64(&collector.saving, -1)

	rcvMsg := gorilla.TSDBpoint{}

	err := json.Unmarshal(buf, &rcvMsg)
	if err != nil {
		gblog.Error(
			"unable to unmarshal udp packet",
			zap.Error(err),
			zap.String("struct", "Collector"),
			zap.String("func", "HandleUDPpacket"),
		)
		return
	}

	ks := "invalid"
	if collector.isKSIDValid(rcvMsg.Tags["ksid"]) {
		ks = rcvMsg.Tags["ksid"]
	}

	collector.udpLimiter.mtx.RLock()
	l, found := collector.udpLimiter.limite[ks]
	collector.udpLimiter.mtx.RUnlock()
	if !found {
		li := rate.NewLimiter(
			rate.Limit(collector.settings.MaxConcurrentUDPPoints),
			int(collector.settings.MaxConcurrentUDPPoints)*2,
		)
		collector.udpLimiter.mtx.Lock()
		collector.udpLimiter.limite[ks] = li
		collector.udpLimiter.mtx.Unlock()
		l = li
	}
	r := l.Reserve()
	if !r.OK() {
		statsUDPRate(ks)
		return
	}
	time.Sleep(r.Delay())

	go func() {
		gerr := collector.HandlePointUDP(rcvMsg)
		if gerr != nil {
			gblog.Error(
				gerr.Message(),
				zap.Error(gerr),
				zap.String("struct", "Collector"),
				zap.String("func", "HandleUDPpacket"),
			)
		}
	}()

}

func (collector *Collector) fail(gerr gobol.Error, addr string) {
	defer func() {
		if r := recover(); r != nil {
			gblog.Error(
				fmt.Sprintf("Panic: %v", r),
				zap.String("func", "fail"),
			)
		}
	}()

	fields := gerr.LogFields()
	fields["addr"] = addr

	gblog.Error(gerr.Error(), zap.Any("fields", fields))
}
