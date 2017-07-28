package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
)

type node struct {
	address string
	port    int
	conf    Config
	mtx     sync.RWMutex

	conn   *grpc.ClientConn
	client pb.TimeseriesClient
}

func newNode(address string, port int, conf Config) (*node, gobol.Error) {

	//cred, err := newClientTLSFromFile(conf.Consul.CA, conf.Consul.Cert, conf.Consul.Key, "*")
	cred, err := credentials.NewClientTLSFromFile(conf.Consul.Cert, "localhost.consul.macs.intranet")
	if err != nil {
		return nil, errInit("newNode", err)
	}

	conn, err := grpc.Dial(fmt.Sprintf("%v:%d", address, port), grpc.WithTransportCredentials(cred))
	if err != nil {
		return nil, errInit("newNode", err)
	}

	logger.Debug(
		"new node",
		zap.String("package", "cluster"),
		zap.String("func", "newNode"),
		zap.String("addr", address),
		zap.Int("port", port),
	)

	return &node{
		address: address,
		port:    port,
		conf:    conf,
		conn:    conn,
		client:  pb.NewTimeseriesClient(conn),
	}, nil
}

func (n *node) write(p *pb.TSPoint) gobol.Error {

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	_, err := n.client.Write(ctx, p)
	if err != nil {
		return errRequest("node/write", http.StatusInternalServerError, err)
	}

	return nil
}

func (n *node) read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	resp, err := n.client.Read(context.Background(), &pb.Query{Ksid: ksid, Tsid: tsid, Start: start, End: end})

	if err != nil {
		return []*pb.Point{}, errRequest("node/read", http.StatusInternalServerError, err)
	}

	return resp.GetPts(), nil

}

func (n *node) meta(m *pb.Meta) (bool, gobol.Error) {
	mf, err := n.client.GetMeta(context.Background(), m)

	if err != nil {
		return false, errRequest("node/meta", http.StatusInternalServerError, err)
	}

	return mf.Ok, nil
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
