package balance

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type GeeRegistryDiscovery struct {
	*MultiServerDiscovery
	registry   string        // 注册中心地址
	timeout    time.Duration // 服务列表过期时间
	lastUpdate time.Time     // 最后从注册中心更新服务列表的时间 默认是10S
}

const defaultUpdateTimeout = time.Second * 10

func NewGeeRegistrtDiscovery(registerAddr string, timeout time.Duration) *GeeRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &GeeRegistryDiscovery{
		MultiServerDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:             registerAddr,
		timeout:              timeout,
	}
	return d
}

func (g *GeeRegistryDiscovery) Update(servers []string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.servers = servers
	g.lastUpdate = time.Now()
	return nil
}

func (g *GeeRegistryDiscovery) Refresh() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lastUpdate.Add(g.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry:", g.registry)
	resp, err := http.Get(g.registry)
	if err != nil {
		log.Println("rpc registry refresh err, ", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Geerpc-Servers"), ",")
	g.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			g.servers = append(g.servers, strings.TrimSpace(server))
		}
	}
	g.lastUpdate = time.Now()
	return nil
}

func (g *GeeRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := g.Refresh(); err != nil {
		return "", err
	}
	return g.MultiServerDiscovery.Get(mode)
}
func (g *GeeRegistryDiscovery) GetAll() ([]string, error) {
	if err := g.Refresh(); err != nil {
		return nil, err
	}
	return g.MultiServerDiscovery.GetAll()
}
