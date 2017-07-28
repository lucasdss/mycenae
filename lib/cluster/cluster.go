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

func (c *Cluster) WAL(p *pb.TSPoint) gobol.Error {

	nodeID, err := c.ch.Get([]byte(p.Tsid))
	if err != nil {
		return errRequest("Write", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		gerr := c.s.WAL(p)
		if err != nil {
			logger.Error(
				"unable to write in local node",
				zap.String("package", "cluster"),
				zap.String("func", "WAL"),
				zap.String("nodeID", nodeID),
				zap.Error(gerr),
			)
			return gerr
		}

		return nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	logger.Debug(
		"forwarding point",
		zap.String("package", "cluster"),
		zap.String("func", "WAL"),
		zap.String("addr", node.address),
		zap.Int("port", node.port),
	)

	return node.write(p)

}

func (c *Cluster) Write(pts []*pb.TSPoint) gobol.Error {

	for _, p := range pts {

		if p != nil {

			nodeID, err := c.ch.Get([]byte(p.GetTsid()))
			if err != nil {
				return errRequest("Write", http.StatusInternalServerError, err)
			}

			if nodeID == c.self {
				gerr := c.s.Write(p)
				if err != nil {
					return gerr
				}

				continue
			}

			c.nMutex.RLock()
			node := c.nodes[nodeID]
			c.nMutex.RUnlock()

			logger.Debug(
				"forwarding point",
				zap.String("package", "cluster"),
				zap.String("func", "Write"),
				zap.String("addr", node.address),
				zap.Int("port", node.port),
			)

			go func(p *pb.TSPoint) {
				// Add WAL for future replay
				err := node.write(p)
				if err != nil {
					logger.Error(
						"remote write",
						zap.String("package", "cluster"),
						zap.String("func", "Write"),
						zap.String("addr", node.address),
						zap.Int("port", node.port),
						zap.Error(err),
					)
				}
			}(p)
		}

	}
	return nil
}

func (c *Cluster) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	ctxt := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "Read"),
	)

	nodeID, err := c.ch.Get([]byte(tsid))
	if err != nil {
		return nil, errRequest("Read", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		ctxt.Debug("reading from local node")
		return c.s.Read(ksid, tsid, start, end)
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	ctxt.Debug(
		"forwarding read",
		zap.String("addr", node.address),
		zap.Int("port", node.port),
	)

	return node.read(ksid, tsid, start, end)
}

func (c *Cluster) shard() {
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

}

func (c *Cluster) Meta(m *pb.Meta) (bool, gobol.Error) {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("func", "Meta"),
	)

	nodeID, err := c.ch.Get([]byte(m.GetKsid()))
	if err != nil {
		return false, errRequest("Meta", http.StatusInternalServerError, err)
	}

	if nodeID == c.self {
		//log.Debug("saving meta in local node")
		c.m.Handle(m)
		return false, nil
	}

	c.nMutex.RLock()
	node := c.nodes[nodeID]
	c.nMutex.RUnlock()

	log.Debug(
		"forwarding meta read",
		zap.String("addr", node.address),
		zap.Int("port", node.port),
	)

	return node.meta(m)
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
