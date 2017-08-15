package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"golang.org/x/net/netutil"

	"github.com/pkg/errors"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/meta"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/tap"
)

type server struct {
	storage    *gorilla.Storage
	meta       *meta.Meta
	grpcServer *grpc.Server
	wLimiter   *rate.Limiter
	rLimiter   *rate.Limiter
	limiter    *rate.Limiter
	workerChan chan workerMsg
}

type workerMsg struct {
	errChan chan error
	p       *pb.TSPoint
}

func newServer(conf Config, strg *gorilla.Storage, m *meta.Meta) (*server, error) {

	s := &server{
		storage:    strg,
		meta:       m,
		wLimiter:   rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.9, conf.GrpcBurstServerConn),
		rLimiter:   rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.1, conf.GrpcBurstServerConn),
		limiter:    rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn), conf.GrpcBurstServerConn),
		workerChan: make(chan workerMsg, conf.GrpcMaxServerConn),
	}

	go func(s *server, conf Config) {
		for {
			grpcServer, lis, err := s.connect(conf)
			if err != nil {
				grpclog.Printf("Unable to connect: %v", err)
				time.Sleep(time.Second)
				continue
			}
			s.grpcServer = grpcServer
			err = s.grpcServer.Serve(lis)
			if err != nil {
				grpclog.Printf("grpc server problem: %v", err)
				s.grpcServer.Stop()
				time.Sleep(time.Second)
				continue
			}

		}
	}(s, conf)

	for i := 0; i < int(conf.GrpcMaxServerConn); i++ {
		s.worker()
	}

	return s, nil
}

func (s *server) connect(conf Config) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		return nil, nil, err
	}
	lis = netutil.LimitListener(lis, conf.MaxListenerConn)

	logger.Debug(
		"loading server keys",
		zap.String("cert", conf.Consul.Cert),
		zap.String("key", conf.Consul.Key),
	)
	c, err := credentials.NewServerTLSFromFile(conf.Consul.Cert, conf.Consul.Key)
	if err != nil {
		return nil, nil, err
	}

	maxStream := uint32(conf.GrpcBurstServerConn) + uint32(conf.GrpcMaxServerConn)

	gServer := grpc.NewServer(
		grpc.Creds(c),
		ServerInterceptor(),
		grpc.InTapHandle(s.rateLimiter),
		grpc.MaxConcurrentStreams(maxStream),
	)

	pb.RegisterTimeseriesServer(gServer, s)

	return gServer, lis, nil

}

func (s *server) rateLimiter(ctx context.Context, info *tap.Info) (context.Context, error) {

	var limiter *rate.Limiter
	switch info.FullMethodName {
	case "/proto.Timeseries/Write":
		limiter = s.wLimiter
	case "/proto.Timeseries/Read":
		limiter = s.rLimiter
	default:
		limiter = s.limiter
	}

	if !limiter.Allow() {
		return nil, errors.New("too many requests, grpc server busy")
	}

	return ctx, nil
}

func (s *server) Write(ctx context.Context, p *pb.TSPoint) (*pb.TSErr, error) {

	/*
		log := logger.With(
			zap.String("package", "cluster"),
			zap.String("func", "server/Write"),
			zap.String("ksid", p.GetKsid()),
			zap.String("tsid", p.GetTsid()),
		)
	*/

	_, ok := ctx.Deadline()
	if !ok {
		return &pb.TSErr{}, errors.New("missing ctx with timeout")
	}

	/*
		timeout := d.Sub(time.Now())

		if timeout < time.Second {
			log.Error("grpc server has not enough time to save", zap.Duration("timeout", timeout))
			return &pb.TSErr{}, fmt.Errorf("grpc server has not enough time to save: %v", timeout)
		}
	*/

	c := make(chan error, 1)

	s.workerChan <- workerMsg{errChan: c, p: p}

	select {
	case err := <-c:
		if err != nil {
			return &pb.TSErr{}, err
		}
	case <-ctx.Done():
		//log.Error("grpc communication problem", zap.Error(ctx.Err()))
		return &pb.TSErr{}, ctx.Err()
	}

	return &pb.TSErr{}, nil
}

func (s *server) worker() {

	go func() {
		for {
			msg := <-s.workerChan
			msg.errChan <- s.storage.Write(msg.p)
		}
	}()

}

func (s *server) Read(ctx context.Context, q *pb.Query) (*pb.Response, error) {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "server/Read"),
		zap.String("ksid", q.GetKsid()),
		zap.String("tsid", q.GetTsid()),
		zap.Int64("start", q.GetStart()),
		zap.Int64("end", q.GetEnd()),
	)

	_, ok := ctx.Deadline()
	if !ok {
		return &pb.Response{}, errors.New("missing ctx with timeout")
	}

	cErr := make(chan error, 1)
	cPts := make(chan []*pb.Point, 1)

	go func() {
		defer close(cErr)
		defer close(cPts)

		pts, err := s.storage.Read(q.GetKsid(), q.GetTsid(), q.GetStart(), q.GetEnd())

		if err != nil {
			cErr <- err
			return
		}

		cPts <- pts

	}()

	select {
	case pts := <-cPts:
		return &pb.Response{Pts: pts}, nil

	case err := <-cErr:
		return &pb.Response{}, err

	case <-ctx.Done():
		log.Error("grpc communication problem", zap.Error(ctx.Err()))
		return &pb.Response{}, ctx.Err()
	}

}

func (s *server) GetMeta(ctx context.Context, m *pb.Meta) (*pb.MetaFound, error) {

	return &pb.MetaFound{Ok: s.meta.Handle(m)}, nil
}

func newServerTLSFromFile(cafile, certfile, keyfile string) (credentials.TransportCredentials, error) {

	cp := x509.NewCertPool()

	data, err := ioutil.ReadFile(cafile)
	if err != nil {
		return nil, fmt.Errorf("Failed to read CA file: %v", err)
	}

	if !cp.AppendCertsFromPEM(data) {
		return nil, errors.New("Failed to parse any CA certificates")
	}

	cert, err := tls.LoadX509KeyPair(certfile, keyfile)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(
		&tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            cp,
			InsecureSkipVerify: true,
			//ClientAuth:   tls.RequireAndVerifyClientCert,
		}), nil

}

func ServerInterceptor() grpc.ServerOption {
	return grpc.UnaryInterceptor(serverInterceptor)
}
func serverInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	logger.Debug(
		"invoke grpc server",
		zap.String("method", info.FullMethod),
		zap.Duration("duration", time.Since(start)),
		zap.Error(err),
	)
	return resp, err
}
