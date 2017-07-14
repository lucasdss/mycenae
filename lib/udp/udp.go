package udp

import (
	"net"

	"github.com/uol/mycenae/lib/structs"

	"go.uber.org/zap"
)

var (
	gblog *zap.Logger
)

type udpHandler interface {
	HandleUDPpacket(buf []byte, addr string)
	Stop()
}

func New(gbl *zap.Logger, setUDP structs.SettingsUDP, handler udpHandler) *UDPserver {

	gblog = gbl

	return &UDPserver{
		handler:  handler,
		settings: setUDP,
	}
}

type UDPserver struct {
	handler  udpHandler
	settings structs.SettingsUDP
	shutdown bool
	closed   chan struct{}
}

func (us UDPserver) Start() {
	go us.asyncStart()
}

func (us UDPserver) asyncStart() {

	port := ":" + us.settings.Port

	addr, err := net.ResolveUDPAddr("udp", port)

	if err != nil {
		gblog.Fatal("addr: ", zap.Error(err))
	} else {
		gblog.Info("addr: resolved")
	}

	sock, err := net.ListenUDP("udp", addr)

	if err != nil {
		gblog.Fatal("listen: ", zap.Error(err))
	} else {
		gblog.Info("listen: ", zap.String("binded to port", us.settings.Port))
	}

	defer sock.Close()

	err = sock.SetReadBuffer(us.settings.ReadBuffer)

	if err != nil {
		gblog.Fatal("set buffer: ", zap.Error(err))
	} else {
		gblog.Info("set buffer: setted")
	}

	for {
		buf := make([]byte, 1024)

		rlen, addr, err := sock.ReadFromUDP(buf)

		saddr := ""

		if addr != nil {
			saddr = addr.IP.String()
		}
		if err != nil {
			gblog.Sugar().Errorf("read buffer from %s : %s", saddr, zap.Error(err))
		} else {
			go us.handler.HandleUDPpacket(buf[0:rlen], saddr)
		}

		if us.shutdown {
			us.closed <- struct{}{}
			return
		}
	}
}

func (us *UDPserver) Stop() {
	us.shutdown = true
	select {
	case <-us.closed:
		us.handler.Stop()
		return
	}
}
