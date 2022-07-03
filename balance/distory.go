package balance

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// SelectMode 代表不同的负载均衡策略，简单起见，GeeRPC 仅实现 Random 和 RoundRobin 两种策略。
type SelectMode int

const (
	RandomSelect    SelectMode = iota // select random
	RandRobinSelect                   // select using Robbin algorithm
)

//Discovery interface，包含了服务发现所需要的最基本的接口。
type Discovry interface {
	Refresh() error                      // 从注册中心更新服务列表
	Update(servers []string) error       // 手动更新服务列表
	Get(mode SelectMode) (string, error) // 根据负载均衡策略，选择一个服务实例
	GetAll() ([]string, error)           // 返回所有的服务实例
}

var _ Discovry = (*MultiServerDiscovery)(nil)

// 手工维护的服务发现
type MultiServerDiscovery struct {
	r       *rand.Rand
	mu      sync.RWMutex
	servers []string
	index   int // 轮询位置
}

func (m *MultiServerDiscovery) Refresh() error {
	// 手工维护 不需要refresh
	return nil
}

func (m *MultiServerDiscovery) Update(servers []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers = servers
	return nil
}

func (m *MultiServerDiscovery) Get(mode SelectMode) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no avaliable servers")
	}
	switch mode {
	case RandomSelect:
		return m.servers[m.r.Intn(n)], nil
	case RandRobinSelect:
		s := m.servers[m.index%n]
		m.index = (m.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not support select mode")
	}
}

func (m *MultiServerDiscovery) GetAll() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	servers := make([]string, len(m.servers), len(m.servers))
	copy(servers, m.servers)
	return servers, nil
}

func NewMultiServerDiscovery(servers []string) *MultiServerDiscovery {
	d := new(MultiServerDiscovery)
	d.servers = servers
	d.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}
