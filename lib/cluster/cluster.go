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
	"github.com/uol/mycenae/lib/tsstats"
	"go.uber.org/zap"
)

var (
	logger *zap.Logger
	stats  *tsstats.StatsTS
)

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

	LogPath string
}

type state struct {
	add  bool
	time int64
}

func New(
	log *zap.Logger,
	sts *tsstats.StatsTS,
	sto gorilla.Gorilla,
	m meta.MetaData,
	conf *Config,
) (*Cluster, gobol.Error) {

	stats = sts

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

	s, gerr := c.Self()
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

	uptime, err := c.Uptime(s)
	if err != nil {
		return nil, errInit("New", err)
	}

	clr := &Cluster{
		c:      c,
		s:      sto,
		m:      m,
		ch:     consistentHash.New(),
		cfg:    conf,
		apply:  conf.ApplyWait,
		nodes:  map[string]Client{},
		toAdd:  map[string]state{},
		uptime: map[string]int64{s: uptime},
		tag:    conf.Consul.Tag,
		self:   s,
		port:   conf.Port,
		server: server,
	}

	clr.ch.Add(s)
	clr.getNodes()
	clr.checkCluster(ci)

	return clr, nil
}

type Cluster struct {
	s     gorilla.Gorilla
	c     Consul
	m     meta.MetaData
	ch    *consistentHash.ConsistentHash
	cfg   *Config
	apply int64

	server   GrpcServer
	stopServ chan struct{}

	nodes  map[string]Client
	nMutex sync.RWMutex
	toAdd  map[string]state
	uptime map[string]int64
	upMtx  sync.RWMutex

	tag  string
	self string
	port int
}

func (c *Cluster) checkCluster(interval time.Duration) {

	go func() {
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
	}()

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

func (c *Cluster) Write(nodes []string, pts []*pb.Point) gobol.Error {

	if len(nodes) > 1 {
		n0 := nodes[0]
		n1 := nodes[1]

		c.upMtx.RLock()
		uptime0 := c.uptime[n0]
		uptime1 := c.uptime[n1]
		c.upMtx.RUnlock()

		prefered := sortNodes(map[string]int64{n0: uptime0, n1: uptime1})
		if prefered == n1 {
			nodes = []string{n1, n0}
		}

	}

	for i, node := range nodes {

		if node == c.self {
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

			continue
		}

		c.nMutex.RLock()
		client := c.nodes[node]
		c.nMutex.RUnlock()

		// Add WAL for future replay
		if client != nil {
			if i > 0 {
				go client.Write(pts)
				continue
			}
			err := client.Write(pts)
			if err != nil {
				logger.Error(
					"unable to write remotely",
					zap.String("package", "cluster"),
					zap.String("func", "Write"),
					zap.String("node", node),
					zap.Error(err),
				)
			}
		}
	}

	return nil
}

func (c *Cluster) Read(ksid, tsid string, start, end int64) ([]*pb.Point, gobol.Error) {

	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("struct", "Cluster"),
		zap.String("func", "Read"),
		zap.String("ksid", ksid),
		zap.String("tsid", tsid),
		zap.Int64("start", start),
		zap.Int64("end", end),
	)

	nodes, err := c.Classifier([]byte(tsid))
	if err != nil {
		return nil, errRequest("Read", http.StatusInternalServerError, err)
	}

	if len(nodes) > 1 {
		n0 := nodes[0]
		n1 := nodes[1]

		c.upMtx.RLock()
		uptime0 := c.uptime[n0]
		uptime1 := c.uptime[n1]
		c.upMtx.RUnlock()

		prefered := sortNodes(map[string]int64{n0: uptime0, n1: uptime1})
		if prefered == n1 {
			nodes = []string{n1, n0}
		}

		log.Debug(
			"reading serie order",
			zap.String("prefered", prefered),
			zap.String("node0", n0),
			zap.Int64("uptime_node0", uptime0),
			zap.String("node1", n1),
			zap.Int64("uptime_node1", uptime1),
		)
	}

	var pts []*pb.Point
	var gerr gobol.Error
	for _, node := range nodes {

		if node == c.self {
			pts, gerr = c.s.Read(ksid, tsid, start, end)
			if gerr != nil {
				log.Error(gerr.Error(), zap.Error(gerr))
				continue
			}

			return pts, nil
		}

		c.nMutex.RLock()
		n := c.nodes[node]
		c.nMutex.RUnlock()

		if n == nil {
			log.Error(
				"node unavailable",
				zap.String("node", node),
			)
			continue
		}

		pts, gerr = n.Read(ksid, tsid, start, end)
		if gerr != nil {
			log.Error(gerr.Error(), zap.Error(gerr))
			continue
		}

		return pts, nil
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

	if node != nil {
		return node.Meta(metas)
	}

	ch := make(chan *pb.MetaFound)
	defer close(ch)
	return ch, nil
}

func (c *Cluster) getNodes() {
	log := logger.With(
		zap.String("package", "cluster"),
		zap.String("struct", "Cluster"),
		zap.String("func", "getNodes"),
	)

	srvs, err := c.c.Nodes()
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
					if check.ServiceID == srv.Service.ID {
						if check.Status == "passing" {
							uptime, err := c.c.Uptime(srv.Node.ID)
							if err != nil {
								log.Error(err.Error(), zap.Error(err))
							} else {
								c.upMtx.Lock()
								c.uptime[srv.Node.ID] = uptime
								c.upMtx.Unlock()
							}

							if _, ok := c.nodes[srv.Node.ID]; ok {
								continue
							}

							n, gerr := newNode(srv.Node.Address, c.port, c.cfg)
							if gerr != nil {
								log.Error("", zap.Error(gerr))
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

							logger.Debug(
								"node has been added",
								zap.String("nodeIP", srv.Node.Address),
								zap.String("nodeID", srv.Node.ID),
								zap.String("status", check.Status),
								zap.Int("port", c.port),
							)

						} else {
							c.upMtx.Lock()
							c.uptime[srv.Node.ID] = 0
							c.upMtx.Unlock()
						}
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
			c.upMtx.Lock()
			c.uptime[id] = 0
			c.upMtx.Unlock()
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

				c.upMtx.Lock()
				delete(c.uptime, id)
				c.upMtx.Unlock()

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
	c.server.Stop()
}

func sortNodes(m map[string]int64) string {
	var node string
	var x int64
	for name, uptime := range m {
		if x == 0 {
			x = uptime
			node = name
			continue
		}

		if uptime > 0 && uptime < x {
			x = uptime
			node = name
			continue
		}
	}

	return node

}
