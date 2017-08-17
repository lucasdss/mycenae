package cluster

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/billhathaway/consistentHash"
	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/gorilla"
	"github.com/uol/mycenae/lib/meta"
	pb "github.com/uol/mycenae/lib/proto"
	"go.uber.org/zap"
)

var logger *zap.Logger

type Config struct {
	Consul ConsulConfig
	//gRPC port
	Port int
	//Ticker interval to check cluster changes
	CheckInterval string
	//Time, in seconds, to wait before applying cluster changes to consistency hashing
	ApplyWait int64

	GrpcTimeout         string
	gRPCtimeout         time.Duration
	GrpcMaxServerConn   int64
	GrpcBurstServerConn int
	MaxListenerConn     int
}

type state struct {
	add  bool
	time int64
}

func New(log *zap.Logger, sto *gorilla.Storage, m *meta.Meta, conf Config) (*Cluster, gobol.Error) {

	if sto == nil {
		return nil, errInit("New", errors.New("storage can't be nil"))
	}

	ci, err := time.ParseDuration(conf.CheckInterval)
	if err != nil {
		log.Error("", zap.Error(err))
		return nil, errInit("New", err)
	}

	gRPCtimeout, err := time.ParseDuration(conf.GrpcTimeout)
	if err != nil {
		log.Error("", zap.Error(err))
		return nil, errInit("New", err)
	}

	conf.gRPCtimeout = gRPCtimeout

	c, gerr := newConsul(conf.Consul)
	if gerr != nil {
		log.Error("", zap.Error(gerr))
		return nil, gerr
	}

	s, gerr := c.getSelf()
	if gerr != nil {
		log.Error("", zap.Error(gerr))
		return nil, gerr
	}
	log.Debug(
		"self id",
		zap.String("package", "cluster"),
		zap.String("func", "New"),
		zap.String("nodeID", s),
	)

	logger = log

	server, err := newServer(conf, sto, m)
	if err != nil {
		return nil, errInit("New", err)
	}

	clr := &Cluster{
		c:      c,
		s:      sto,
		m:      m,
		ch:     consistentHash.New(),
		cfg:    &conf,
		apply:  conf.ApplyWait,
		nodes:  map[string]*node{},
		toAdd:  map[string]state{},
		tag:    conf.Consul.Tag,
		self:   s,
		port:   conf.Port,
		server: server,
	}

	clr.ch.Add(s)
	clr.getNodes()
	go clr.checkCluster(ci)

	return clr, nil
}

type Cluster struct {
	s     *gorilla.Storage
	c     *consul
	m     *meta.Meta
	ch    *consistentHash.ConsistentHash
	cfg   *Config
	apply int64

	server   *server
	stopServ chan struct{}

	nodes  map[string]*node
	nMutex sync.RWMutex
	toAdd  map[string]state

	tag  string
	self string
	port int
}

func (c *Cluster) checkCluster(interval time.Duration) {

	ticker := time.NewTicker(interval)

	for {
		select {
		case <-ticker.C:
			c.getNodes()
		case <-c.stopServ:
			logger.Debug("stopping", zap.String("package", "cluster"), zap.String("func", "checkCluster"))
			return
		}
	}

}

func (c *Cluster) WAL(pts []*pb.Point) error {

	limiter := make(chan interface{}, 5000)
	defer close(limiter)

	for _, p := range pts {
		limiter <- struct{}{}
		if p == nil {
			continue
		}
		go func(p *pb.Point) {
			gerr := c.s.WAL(p)
			if gerr != nil {
				logger.Error(
					"unable to write in local node",
					zap.String("package", "cluster"),
					zap.String("func", "WAL"),
					zap.Error(gerr),
				)
			}
			<-limiter
		}(p)
	}

	return nil

}

func (c *Cluster) Classifier(tsid []byte) ([]string, gobol.Error) {
	nodes, err := c.ch.GetN(tsid, 2)
	if err != nil {
		logger.Debug("running as single node?", zap.String("consistent hash", err.Error()))
		node, err := c.ch.Get(tsid)
		if err != nil {
			return nil, errRequest("Write", http.StatusInternalServerError, err)
		}
		return []string{node}, nil
	}
	return nodes, nil
}

func (c *Cluster) Write(nodeID string, pts []*pb.Point) gobol.Error {

	if nodeID == c.self {
		go func() {
			for _, p := range pts {
				gerr := c.s.Write(p)
				if gerr != nil {
					logger.Error(
						"unable to write locally",
						zap.String("package", "cluster"),
						zap.String("func", "Write"),
						zap.Error(gerr),
					)
				}
			}
		}()

		return nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	// Add WAL for future replay
	if node != nil {
		err := node.write(pts)
		if err != nil {
			logger.Error(
				"unable to write remotely",
				zap.String("package", "cluster"),
				zap.String("func", "Write"),
				zap.String("node", nodeID),
				zap.Error(err),
			)
		}
	}

	return nil
}

func (c *Cluster) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "Read"),
	)

	node, err := c.ch.Get([]byte(tsid))
	if err != nil {
		return nil, errRequest("Write", http.StatusInternalServerError, err)
	}
	if node == c.self {
		log.Debug("reading from local node")
		pts, gerr := c.s.Read(ksid, tsid, start, end)
		if gerr != nil {
			log.Error(gerr.Error(), zap.Error(gerr))
		}
		return pts, nil
	}

	c.nMutex.RLock()
	n := c.nodes[node]
	c.nMutex.RUnlock()

	log.Debug(
		"forwarding read",
		zap.String("addr", n.address),
		zap.Int("port", n.port),
	)

	var pts []*pb.Point
	var gerr gobol.Error
	pts, gerr = n.read(ksid, tsid, start, end)
	if gerr != nil {
		log.Error(gerr.Error(), zap.Error(gerr))
	} else {
		return pts, gerr
	}

	nodes, err := c.Classifier([]byte(tsid))
	if err != nil {
		return nil, errRequest("Read", http.StatusInternalServerError, err)
	}

	for _, node := range nodes {
		if node == c.self {
			continue
		}
		c.nMutex.RLock()
		n := c.nodes[node]
		c.nMutex.RUnlock()

		log.Debug(
			"forwarding read as fallback",
			zap.String("addr", n.address),
			zap.Int("port", n.port),
		)

		pts, gerr = n.read(ksid, tsid, start, end)
		if gerr != nil {
			log.Error(gerr.Error(), zap.Error(gerr))
			continue
		}

		break
	}

	return pts, gerr

}

func (c *Cluster) shard() {
	/*
		series := c.s.ListSeries()

		for _, s := range series {
			n, err := c.ch.Get([]byte(s.TSID))
			if err != nil {

				logger.Error(
					err.Error(),
					zap.String("package", "cluster"),
					zap.String("func", "shard"),
				)
				continue
			}
			if len(n) > 0 && n != c.self {
				c.nMutex.RLock()
				node := c.nodes[n]
				c.nMutex.RUnlock()

				ptsC := c.s.Delete(s)
					for pts := range ptsC {
						for _, p := range pts {
							node.write(&pb.TSPoint{
								Tsid:  s.TSID,
								Ksid:  s.KSID,
								Date:  p.Date,
								Value: p.Value,
							})
						}
					}
			}
		}
	*/
}

func (c *Cluster) MetaClassifier(ksid []byte) (string, gobol.Error) {
	nodeID, err := c.ch.Get(ksid)
	if err != nil {
		return "", errRequest("MetaClassifier", http.StatusInternalServerError, err)
	}
	return nodeID, nil
}

func (c *Cluster) SelfID() string {
	return c.self
}

func (c *Cluster) Meta(nodeID string, metas []*pb.Meta) (<-chan *pb.MetaFound, error) {

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	logger.Debug(
		"forwarding meta read",
		zap.String("addr", node.address),
		zap.Int("port", node.port),
		zap.String("package", "cluster"),
		zap.String("func", "Meta"),
	)

	return node.meta(metas)

}

func (c *Cluster) getNodes() {
	logger := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "getNodes"),
	)

	srvs, err := c.c.getNodes()
	if err != nil {
		logger.Error("", zap.Error(err))
	}

	now := time.Now().Unix()
	reShard := false

	for _, srv := range srvs {
		if srv.Node.ID == c.self || srv.Node.ID == "" {
			continue
		}

		for _, tag := range srv.Service.Tags {
			if tag == c.tag {
				for _, check := range srv.Checks {
					if check.ServiceID == srv.Service.ID && check.Status == "passing" {
						if _, ok := c.nodes[srv.Node.ID]; ok {
							continue
						}

						n, err := newNode(srv.Node.Address, c.port, *c.cfg)
						if err != nil {
							logger.Error("", zap.Error(err))
							continue
						}

						logger.Debug(
							"adding node",
							zap.String("nodeIP", srv.Node.Address),
							zap.String("nodeID", srv.Node.ID),
							zap.String("status", check.Status),
							zap.Int("port", c.port),
						)

						c.ch.Add(srv.Node.ID)

						c.nMutex.Lock()
						c.nodes[srv.Node.ID] = n
						c.nMutex.Unlock()

						reShard = true

						logger.Debug(
							"node has been added",
							zap.String("nodeIP", srv.Node.Address),
							zap.String("nodeID", srv.Node.ID),
							zap.String("status", check.Status),
							zap.Int("port", c.port),
						)

					}
				}
			}
		}
	}

	del := []string{}

	for id := range c.nodes {
		found := false
		for _, srv := range srvs {
			for _, check := range srv.Checks {
				if check.ServiceID == srv.Service.ID && check.Status == "passing" && id == srv.Node.ID {
					found = true
				}
			}
		}
		if !found {
			del = append(del, id)
		}
	}

	for _, id := range del {

		if s, ok := c.toAdd[id]; ok && !s.add {
			if now-s.time >= c.apply {

				c.ch.Remove(id)

				c.nMutex.Lock()
				delete(c.nodes, id)
				c.nMutex.Unlock()

				delete(c.toAdd, id)
				reShard = true

				logger.Debug("removed node")
			}
		} else {
			c.toAdd[id] = state{
				add:  false,
				time: now,
			}
		}
	}

	if reShard {
		go c.shard()
	}

}

//Stop cluster
func (c *Cluster) Stop() {
	c.stopServ <- struct{}{}
	c.server.grpcServer.Stop()
}
