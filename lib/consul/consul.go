package consul

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/uol/gobol"
)

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

type Consul struct {
	c          *http.Client
	token      string
	serviceAPI string
	agentAPI   string
	healthAPI  string
	kvAPI      string
}

type KVPair struct {
	Key         string
	CreateIndex uint64
	ModifyIndex uint64
	LockIndex   uint64
	Flags       uint64
	Value       []byte
	Session     string
}

type KVPairs []KVPair

type Schema struct {
	Timestamp int64 `json:"timestamp"`
	Total     int   `json:"total"`
}

func New(conf ConsulConfig) (*Consul, gobol.Error) {

	cert, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
	if err != nil {
		return nil, errInit("New", err)
	}

	caCert, err := ioutil.ReadFile(conf.CA)
	if err != nil {
		return nil, errInit("New", err)
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

	return &Consul{
		c: &http.Client{
			Transport: tr,
			Timeout:   time.Second,
		},

		serviceAPI: fmt.Sprintf("%s/v1/catalog/service/%s", address, conf.Service),
		agentAPI:   fmt.Sprintf("%s/v1/agent/self", address),
		healthAPI:  fmt.Sprintf("%s/v1/health/service/%s", address, conf.Service),
		kvAPI:      fmt.Sprintf("%s/v1/kv/", address),
		token:      conf.Token,
	}, nil
}

func (c *Consul) GetNodes() ([]Health, gobol.Error) {

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

func (c *Consul) GetSelf() (string, gobol.Error) {

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

func (c *Consul) GetLock() (bool, gobol.Error) {

	var err error
	var ikeyspaceLocked interface{}

	schemaRaw, gerr := c.readKey("schema")
	if gerr != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, gerr)
	}

	if schemaRaw == nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, errors.New("Schema status not found"))
	}

	var schema Schema
	err = json.Unmarshal(schemaRaw.([]uint8), &schema)
	if err != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, err)
	}

	if time.Now().Unix()-schema.Timestamp > 7200 {
		return false, errRequest("GetLock", http.StatusInternalServerError, errors.New("Schema status was not updated in the last two hours"))
	}

	if schema.Total > 1 {
		return false, errRequest("GetLock", http.StatusInternalServerError, errors.New("More than 1 schema was found"))
	}

	ikeyspaceLocked, err = c.readKey("keyspaceLocked")
	if err != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, err)
	}

	i := 0
	for ikeyspaceLocked != nil {
		ikeyspaceLocked, err = c.readKey("keyspaceLocked")
		if err != nil {
			return false, errRequest("GetLock", http.StatusInternalServerError, err)
		}

		if i == 60 {
			return false, errRequest("GetLock", http.StatusLocked, nil)
		}

		time.Sleep(time.Second)
		i++
	}

	name, err := os.Hostname()
	if err != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, err)
	}

	req, err := http.NewRequest("PUT", c.kvAPI+"keyspaceLocked", strings.NewReader(name))
	if err != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return false, errRequest("GetLock", http.StatusInternalServerError, err)
	}

	if resp.StatusCode >= 300 {
		return false, errRequest("GetLock", resp.StatusCode, err)
	}

	return true, nil
}

func (c *Consul) ReleaseLock() (bool, gobol.Error) {

	req, err := http.NewRequest("DELETE", c.kvAPI+"keyspaceLocked", nil)
	if err != nil {
		return false, errRequest("ReleaseLock", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return false, errRequest("ReleaseLock", http.StatusInternalServerError, err)
	}

	if resp.StatusCode >= 300 {
		return false, errRequest("ReleaseLock", resp.StatusCode, err)
	}

	return true, nil
}

func (c *Consul) readKey(key string) (interface{}, gobol.Error) {

	req, err := http.NewRequest("GET", c.kvAPI+key, nil)
	if err != nil {
		return nil, errRequest("readKey", http.StatusInternalServerError, err)
	}
	req.Header.Add("X-Consul-Token", c.token)

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, errRequest("readKey", http.StatusInternalServerError, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode >= 300 {
		return nil, errRequest("readKey", resp.StatusCode, err)
	}

	dec := json.NewDecoder(resp.Body)

	var value KVPairs

	err = dec.Decode(&value)
	if err != nil {
		return nil, errRequest("readKey", http.StatusInternalServerError, err)
	}

	return value[0].Value, nil
}
