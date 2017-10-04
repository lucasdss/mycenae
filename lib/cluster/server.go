package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"time"

	"golang.org/x/net/netutil"

	"github.com/pkg/errors"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/limiter"
	"github.com/uol/mycenae/lib/meta"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

type GrpcServer interface {
	Write(stream pb.Timeseries_WriteServer) error
	Read(q *pb.Query, stream pb.Timeseries_ReadServer) error
	WriteMeta(stream pb.Timeseries_WriteMetaServer) error
	Stop()
}

type server struct {
	storage    gorilla.Gorilla
	meta       meta.MetaData
	grpcServer *grpc.Server
	wLimiter   *limiter.RateLimit
	rLimiter   *limiter.RateLimit
	mLimiter   *limiter.RateLimit
}

type workerMsg struct {
	errChan chan error
	p       *pb.Point
}

func newServer(conf *Config, strg gorilla.Gorilla, m meta.MetaData) (GrpcServer, error) {

	w, gerr := limiter.New(int(conf.GrpcMaxServerConn/10)*8, int(conf.GrpcBurstServerConn/10)*8, logger)
	if gerr != nil {
		return nil, gerr
	}
	r, gerr := limiter.New(int(conf.GrpcMaxServerConn/10), int(conf.GrpcBurstServerConn/10), logger)
	if gerr != nil {
		return nil, gerr
	}

	ml, gerr := limiter.New(int(conf.GrpcMaxServerConn/10), int(conf.GrpcBurstServerConn/10), logger)
	if gerr != nil {
		return nil, gerr
	}

	s := &server{
		storage:  strg,
		meta:     m,
		wLimiter: w,
		rLimiter: r,
		mLimiter: ml,
	}

	go func(s *server, conf *Config) {
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

	return s, nil
}

func (s *server) Stop() {
	s.grpcServer.Stop()
}

func (s *server) connect(conf *Config) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		return nil, nil, err
	}
	lis = netutil.LimitListener(lis, conf.MaxListenerConn)

	/*

		logger.Debug(
			"loading server keys",
			zap.String("cert", conf.Consul.Cert),
			zap.String("key", conf.Consul.Key),
		)
			c, err := credentials.NewServerTLSFromFile(conf.Consul.Cert, conf.Consul.Key)
			if err != nil {
				return nil, nil, err
			}
	*/

	maxStream := uint32(conf.GrpcBurstServerConn) + uint32(conf.GrpcMaxServerConn)

	gServer := grpc.NewServer(
		//grpc.Creds(c),
		ServerInterceptor(),
		grpc.InTapHandle(s.rateLimiter),
		grpc.MaxConcurrentStreams(maxStream),
	)

	pb.RegisterTimeseriesServer(gServer, s)

	return gServer, lis, nil

}

func (s *server) rateLimiter(ctx context.Context, info *tap.Info) (context.Context, error) {

	var limiter *limiter.RateLimit
	switch info.FullMethodName {
	case "/proto.Timeseries/Write":
		limiter = s.wLimiter
	case "/proto.Timeseries/Read":
		limiter = s.rLimiter
	case "/proto.Timeseries/WriteMeta":
		limiter = s.mLimiter
	default:
		limiter = s.wLimiter
	}

	if err := limiter.Reserve(); err != nil {
		logger.Error(
			err.Message(),
			zap.Error(err),
			zap.String("package", "cluster"),
			zap.String("struct", "server"),
			zap.String("func", "rateLimiter"),
		)
		return nil, err
	}

	return ctx, nil
}

func (s *server) Write(stream pb.Timeseries_WriteServer) error {

	ctx := stream.Context()

	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing ctx with timeout")
	}

	c := make(chan error, 1)

	go func() {
		defer close(c)

		for {
			p, err := stream.Recv()
			if err == io.EOF {
				c <- nil
				return
			}
			if err != nil {
				logger.Error(
					"problem to save point while writing through gRPC",
					zap.String("func", "server/Write"),
					zap.String("package", "cluster"),
					zap.Error(err),
				)
				c <- err
				return
			}

			if gerr := s.storage.Write(p); gerr != nil {
				c <- err
				return
			}

		}

	}()

	select {
	case err := <-c:
		if err != nil {
			logger.Error(
				"gorilla storage problem",
				zap.String("func", "server/Write"),
				zap.String("package", "cluster"),
				zap.Error(err),
			)
		}
		if err := stream.SendAndClose(&pb.TSResponse{}); err != nil {
			logger.Error(
				"unable to send close stream",
				zap.String("func", "server/Write"),
				zap.String("package", "cluster"),
				zap.Error(err),
			)

		}
		return err

	case <-ctx.Done():
		return ctx.Err()
	}

}

// Read(*Query, Timeseries_ReadServer)
func (s *server) Read(q *pb.Query, stream pb.Timeseries_ReadServer) error {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("struct", "server"),
		zap.String("func", "Read"),
		zap.String("ksid", q.GetKsid()),
		zap.String("tsid", q.GetTsid()),
		zap.Int64("start", q.GetStart()),
		zap.Int64("end", q.GetEnd()),
	)

	ctx := stream.Context()
	d, ok := ctx.Deadline()
	if !ok {
		return errors.New("missing ctx with timeout")
	}
	log = log.With(zap.Time("deadline", d))

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
		for _, p := range pts {
			err := stream.Send(p)
			if err != nil {
				log.Error("grpc streaming problem", zap.Error(err))
				return err
			}
		}

		return nil

	case err := <-cErr:
		return err

	case <-ctx.Done():
		log.Error("grpc communication problem", zap.Error(ctx.Err()))
		return ctx.Err()
	}

}

// GetMeta(Timeseries_GetMetaServer)  error
func (s *server) WriteMeta(stream pb.Timeseries_WriteMetaServer) error {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("struct", "server"),
		zap.String("func", "WriteMeta"),
	)

	ctx := stream.Context()
	d, ok := ctx.Deadline()
	if !ok {
		log.Error("missing ctx with timeout")
		return errors.New("missing ctx with timeout")
	}
	log = log.With(zap.Time("deadline", d))

	var c int
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.MetaFound{})
		}
		if err != nil {
			log.Error(err.Error())
			return err
		}
		c++

		select {
		case <-ctx.Done():
			log.Debug("finished", zap.Error(ctx.Err()))
			return ctx.Err()
		default:
			go s.meta.Handle(m)
		}
		log.Debug("meta received", zap.Int("count", c))
	}

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
	return grpc.StreamInterceptor(serverInterceptor)
}

//type StreamServerInterceptor func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error
func serverInterceptor(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {

	start := time.Now()
	defer statsProcTime(info.FullMethod, time.Since(start))

	err := handler(srv, ss)
	status, ok := status.FromError(err)
	if !ok {
		return err
	}
	statsProcCount(info.FullMethod, strconv.Itoa(int(status.Code())))
	if err != nil {
		return err
	}

	return nil
}
