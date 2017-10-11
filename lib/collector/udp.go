package collector

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uol/gobol"
	"github.com/uol/mycenae/lib/gorilla"

	"go.uber.org/zap"
)

func (collector *Collector) HandleUDPpacket(buf []byte, addr string) {

	atomic.AddInt64(&collector.saving, 1)
	defer atomic.AddInt64(&collector.saving, -1)

	rcvMsg := gorilla.TSDBpoint{}

	err := json.Unmarshal(buf, &rcvMsg)
	if err != nil {
		gerr := errUnmarshal("HandleUDPpacket", err)
		gblog.Error(
			gerr.Message(),
			zap.Error(gerr),
			zap.String("struct", "Collector"),
			zap.String("func", "HandleUDPpacket"),
		)

		return
	}

	rl := collector.udpLimiter.Reserve()
	if !rl.OK() {
		ks := "invalid"
		if collector.isKSIDValid(rcvMsg.Tags["ksid"]) {
			ks = rcvMsg.Tags["ksid"]
		}
		statsUDPRate(ks)
		return
	}
	time.Sleep(rl.Delay())

	gerr := collector.HandlePointUDP(rcvMsg)
	if gerr != nil {
		gblog.Error(
			gerr.Message(),
			zap.Error(gerr),
			zap.String("struct", "Collector"),
			zap.String("func", "HandleUDPpacket"),
		)
	}

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
