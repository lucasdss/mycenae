package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/meta"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

type server struct {
	storage    *gorilla.Storage
	meta       *meta.Meta
	grpcServer *grpc.Server
}

func newServer(conf Config, strg *gorilla.Storage, m *meta.Meta) (*server, error) {

	s := &server{storage: strg, meta: m}

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

	return s, nil
}

func (s *server) connect(conf Config) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.Port))
	if err != nil {
		return nil, nil, err
	}

	//c, err := newServerTLSFromFile(conf.Consul.CA, conf.Consul.Cert, conf.Consul.Key)
	logger.Debug(
		"loading server keys",
		zap.String("cert", conf.Consul.Cert),
		zap.String("key", conf.Consul.Key),
	)
	c, err := credentials.NewServerTLSFromFile(conf.Consul.Cert, conf.Consul.Key)
	if err != nil {
		return nil, nil, err
	}
	opts := []grpc.ServerOption{grpc.Creds(c)}

	gServer := grpc.NewServer(opts...)

	pb.RegisterTimeseriesServer(gServer, s)

	return gServer, lis, nil

}

func (s *server) Write(ctx context.Context, p *pb.TSPoint) (*pb.TSErr, error) {

	logger.Debug(
		"grpc server writing",
		zap.String("package", "cluster"),
		zap.String("func", "server/Write"),
		zap.String("ksid", p.GetKsid()),
		zap.String("tsid", p.GetTsid()),
	)

	err := s.storage.Write(p)
	if err != nil {
		return &pb.TSErr{}, err
	}

	return &pb.TSErr{}, nil
}

func (s *server) Read(ctx context.Context, q *pb.Query) (*pb.Response, error) {

	pts, err := s.storage.Read(q.GetKsid(), q.GetTsid(), q.GetStart(), q.GetEnd())

	return &pb.Response{Pts: pts}, err
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
