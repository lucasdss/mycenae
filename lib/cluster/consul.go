package cluster

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/uol/gobol"
)

type Consul interface {
	Nodes() ([]Health, gobol.Error)
	Self() (string, gobol.Error)
	Uptime(node string) (int64, error)
}

type ConsulConfig struct {
	//Consul agent adrress without the scheme
	Address string
	//Consul agent port
	Port int
	//Location of consul agent cert file
	Cert string
	//Location of consul agent key file
	Key string
	//Location of consul agent CA file
	CA string
	//Name of the service to be probed on consul
	Service string
	//Tag of the service
	Tag string
	// Token of the service
	Token string
	// Protocol of the service
	Protocol string
}

type Health struct {
	Node    Node    `json:"Node"`
	Service Service `json:"Service"`
	Checks  []Check `json:"Checks"`
}

type Node struct {
	ID              string            `json:"ID"`
	Node            string            `json:"Node"`
	Address         string            `json:"Address"`
	TaggedAddresses TagAddr           `json:"TaggedAddresses"`
	Meta            map[string]string `json:"Meta"`
	CreateIndex     int               `json:"CreateIndex"`
	ModifyIndex     int               `json:"ModifyIndex"`
}

type TagAddr struct {
	Lan string `json:"lan"`
	Wan string `json:"wan"`
}

type Service struct {
	ID                string   `json:"ID"`
	Service           string   `json:"Service"`
	Tags              []string `json:"Tags"`
	Address           string   `json:"Address"`
	Port              int      `json:"Port"`
	EnableTagOverride bool     `json:"EnableTagOverride"`
	CreateIndex       int      `json:"CreateIndex"`
	ModifyIndex       int      `json:"ModifyIndex"`
}

type Check struct {
	Node        string `json:"Node"`
	CheckID     string `json:"CheckID"`
	Name        string `json:"Name"`
	Status      string `json:"Status"`
	Notes       string `json:"Notes"`
	Output      string `json:"Output"`
	ServiceID   string `json:"ServiceID"`
	ServiceName string `json:"ServiceName"`
	CreateIndex int    `json:"CreateIndex"`
	ModifyIndex int    `json:"ModifyIndex"`
}

type Addresses struct {
	Lan string `json:"lan"`
	Wan string `json:"wan"`
}

type Local struct {
	Config Conf `json:"Config"`
}

type Conf struct {
	NodeID string `json:"NodeID"`
}

func newConsul(conf ConsulConfig) (Consul, gobol.Error) {

	cert, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
	if err != nil {
		return nil, errInit("newConsul", err)
	}

	caCert, err := ioutil.ReadFile(conf.CA)
	if err != nil {
		return nil, errInit("newConsul", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		},
		DisableKeepAlives:   false,
		MaxIdleConns:        1,
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     5 * time.Second,
	}
	defer tr.CloseIdleConnections()

	address := fmt.Sprintf("%s://%s:%d", conf.Protocol, conf.Address, conf.Port)

	c := &consul{
		c: &http.Client{
			Transport: tr,
			Timeout:   time.Second,
		},

		serviceAPI: fmt.Sprintf("%s/v1/catalog/service/%s", address, conf.Service),
		agentAPI:   fmt.Sprintf("%s/v1/agent/self", address),
		healthAPI:  fmt.Sprintf("%s/v1/health/service/%s", address, conf.Service),
		uptimeAPI:  fmt.Sprintf("%s/v1/kv/%s", address, conf.Service),
		token:      conf.Token,
	}

	s, err := c.Self()
	if err != nil {
		return nil, errInit("newConsul", err)
	}
	c.serlf = s

	if gerr := c.setUptime(s); gerr != nil {
		return nil, gerr
	}

	return c, nil

}

type consul struct {
	c          *http.Client
	serlf      string
	token      string
	serviceAPI string
	agentAPI   string
	healthAPI  string
	uptimeAPI  string
}

func (c *consul) Nodes() ([]Health, gobol.Error) {

	req, err := http.NewRequest("GET", c.healthAPI, nil)
	if err != nil {
		return nil, errRequest("getNodes", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, errRequest("getNodes", http.StatusInternalServerError, err)
	}

	dec := json.NewDecoder(resp.Body)

	srvs := []Health{}

	err = dec.Decode(&srvs)
	if err != nil {
		return nil, errRequest("getNodes", http.StatusInternalServerError, err)
	}

	return srvs, nil
}

func (c *consul) Self() (string, gobol.Error) {

	if len(c.serlf) > 1 {
		return c.serlf, nil
	}

	req, err := http.NewRequest("GET", c.agentAPI, nil)
	if err != nil {
		return "", errRequest("getSelf", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return "", errRequest("getSelf", http.StatusInternalServerError, err)
	}

	if resp.StatusCode >= 300 {
		return "", errRequest("getSelf", resp.StatusCode, err)
	}

	dec := json.NewDecoder(resp.Body)

	self := Local{}

	err = dec.Decode(&self)
	if err != nil {
		return "", errRequest("getSelf", http.StatusInternalServerError, err)
	}

	return self.Config.NodeID, nil
}

type KV struct {
	Value string `json:"Value"`
}

func (c *consul) Uptime(node string) (int64, error) {

	url := fmt.Sprintf("%s/%s", c.uptimeAPI, node)

	var uptime int64
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, err)
	}

	if resp.StatusCode >= 300 {
		return uptime, errRequest("Uptime", resp.StatusCode, errors.New(resp.Status))
	}

	dec := json.NewDecoder(resp.Body)

	body := []KV{}

	err = dec.Decode(&body)
	if err != nil {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, err)
	}

	if len(body) == 0 || len(body) > 1 {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, errors.New("wrong number of objects from kv"))
	}

	data, err := base64.StdEncoding.DecodeString(body[0].Value)
	if err != nil {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, err)
	}

	uptime, err = strconv.ParseInt(string(data), 10, 64)

	if err != nil {
		return uptime, errRequest("Uptime", http.StatusInternalServerError, err)
	}

	return uptime, nil

}

func (c *consul) setUptime(node string) gobol.Error {

	uptime := strconv.FormatInt(time.Now().Unix(), 10)
	data := bytes.NewBufferString(uptime)

	url := fmt.Sprintf("%s/%s", c.uptimeAPI, node)

	req, err := http.NewRequest("PUT", url, data)
	if err != nil {
		return errRequest("setUptime", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return errRequest("setUptime", http.StatusInternalServerError, err)
	}

	if resp.StatusCode >= 300 {
		return errRequest("setUptime", resp.StatusCode, err)
	}

	return nil

}
