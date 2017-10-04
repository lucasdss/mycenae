package collector

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

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

	if gerr := collector.udpLimiter.Reserve(); gerr != nil {
		gblog.Error(
			"too many udp requests",
			zap.String("struct", "Collector"),
			zap.String("func", "HandleUDPpacket"),
			zap.Error(gerr),
		)

		return
	}

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
