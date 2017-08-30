package cluster

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"

	"golang.org/x/time/rate"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/wal"
	"go.uber.org/zap"
)

type node struct {
	address string
	port    int
	conf    *Config
	mtx     sync.RWMutex
	ptsCh   chan []*pb.Point
	metaCh  chan []*pb.Meta
	pts     []*pb.Point

	conn     *grpc.ClientConn
	client   pb.TimeseriesClient
	wLimiter *rate.Limiter
	rLimiter *rate.Limiter
	mLimiter *rate.Limiter
	wal      *wal.WAL
}

func newNode(address string, port int, conf *Config, walConf *wal.Settings) (*node, gobol.Error) {

	//cred, err := newClientTLSFromFile(conf.Consul.CA, conf.Consul.Cert, conf.Consul.Key, "*")
	cred, err := credentials.NewClientTLSFromFile(conf.Consul.Cert, "localhost.consul.macs.intranet")
	if err != nil {
		return nil, errInit("newNode", err)
	}

	conn, err := grpc.Dial(fmt.Sprintf("%v:%d", address, port), grpc.WithTransportCredentials(cred))
	if err != nil {
		return nil, errInit("newNode", err)
	}

	wSettings := &wal.Settings{
		PathWAL:       filepath.Join(walConf.PathWAL, address),
		SyncInterval:  walConf.SyncInterval,
		MaxBufferSize: walConf.MaxBufferSize,
		MaxConcWrite:  walConf.MaxConcWrite,
	}

	wal, err := wal.New(wSettings, logger)

	logger.Debug(
		"new node",
		zap.String("package", "cluster"),
		zap.String("func", "newNode"),
		zap.String("addr", address),
		zap.Int("port", port),
	)

	node := &node{
		address:  address,
		port:     port,
		conf:     conf,
		conn:     conn,
		wal:      wal,
		ptsCh:    make(chan []*pb.Point, 5),
		metaCh:   make(chan []*pb.Meta, 5),
		wLimiter: rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.9, conf.GrpcBurstServerConn),
		rLimiter: rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.1, conf.GrpcBurstServerConn),
		mLimiter: rate.NewLimiter(rate.Limit(conf.GrpcMaxServerConn)*0.1, conf.GrpcBurstServerConn),
		client:   pb.NewTimeseriesClient(conn),
	}

	return node, nil
}

func (n *node) write(pts []*pb.Point) error {

	ctx, cancel := context.WithTimeout(context.Background(), n.conf.gRPCtimeout)
	defer cancel()

	if err := n.wLimiter.Wait(ctx); err != nil {
		logger.Error(
			"write limit exceeded",
			zap.String("package", "cluster"),
			zap.String("func", "write"),
			zap.String("error", err.Error()),
		)
		// send to wal
		//n.wal.Add(p *proto.Point)
		return err
	}

	stream, err := n.client.Write(ctx)
	if err != nil {
		logger.Error(
			"failed to get stream from server",
			zap.String("package", "cluster"),
			zap.String("func", "write"),
			zap.Error(err),
		)
		// send to wal
		return err
	}

	for _, p := range pts {
		var attempts int
		var err error
		for {
			attempts++
			err = stream.Send(p)
			if err == io.EOF {
				return nil
			}
			if err == nil {
				break
			}

			logger.Error(
				"retry write stream",
				zap.String("package", "cluster"),
				zap.String("func", "write"),
				zap.Int("attempt", attempts),
				zap.Error(err),
			)
			if attempts >= 5 {
				break
			}
		}

		if err != nil && err != io.EOF {
			logger.Error(
				"too many attempts, unable to write to stream",
				zap.String("package", "cluster"),
				zap.String("func", "write"),
				zap.Error(err),
			)
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil && err != io.EOF {
		logger.Error(
			"problem writing to stream",
			zap.String("package", "cluster"),
			zap.String("func", "write"),
			zap.Error(err),
		)
	}

	return nil

}

func (n *node) read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	ctx, cancel := context.WithTimeout(context.Background(), n.conf.gRPCtimeout)
	defer cancel()

	if err := n.rLimiter.Wait(ctx); err != nil {
		return []*pb.Point{}, errRequest("node/read", http.StatusInternalServerError, err)
	}

	stream, err := n.client.Read(ctx, &pb.Query{Ksid: ksid, Tsid: tsid, Start: start, End: end})
	if err != nil {
		return []*pb.Point{}, errRequest("node/read", http.StatusInternalServerError, err)
	}

	var pts []*pb.Point
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			// read done.
			return pts, nil
		}
		if err != nil {
			return pts, errRequest("node/write", http.StatusInternalServerError, err)
		}

		pts = append(pts, p)
	}

}

func (n *node) meta(metas []*pb.Meta) (<-chan *pb.MetaFound, error) {

	ctx, cancel := context.WithTimeout(context.Background(), n.conf.gRPCtimeout)

	if err := n.mLimiter.Wait(ctx); err != nil {
		logger.Error(
			"meta request limit",
			zap.String("package", "cluster"),
			zap.String("func", "node/meta"),
			zap.Error(err),
		)
		return nil, err
	}

	stream, err := n.client.GetMeta(ctx)
	if err != nil {
		logger.Error(
			"meta gRPC problem",
			zap.String("package", "cluster"),
			zap.String("func", "node/meta"),
			zap.Error(err),
		)
		return nil, err
	}

	go func() {
		for _, m := range metas {
			err := stream.Send(m)
			if err == io.EOF {
				return
			}
			if err != nil {
				logger.Error(
					"meta gRPC send problem",
					zap.String("package", "cluster"),
					zap.String("func", "node/meta"),
					zap.Error(err),
				)
			}
		}

		err := stream.CloseSend()
		if err != nil {
			logger.Error(
				"meta gRPC CloseSend problem",
				zap.String("package", "cluster"),
				zap.String("func", "node/meta"),
				zap.Error(err),
			)
		}
	}()

	c := make(chan *pb.MetaFound, len(metas))
	go func() {
		defer close(c)
		defer cancel()
		for _ = range metas {
			mf, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				logger.Error(
					"meta gRPC receive problem",
					zap.String("package", "cluster"),
					zap.String("func", "node/meta"),
					zap.Error(err),
				)
				continue
			}

			c <- mf
		}
	}()

	return c, nil

}

func (n *node) close() {
	err := n.conn.Close()
	if err != nil {
		logger.Error(
			"closing connection",
			zap.String("package", "cluster"),
			zap.String("func", "node/close"),
			zap.Error(err),
		)
	}
}
