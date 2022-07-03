package registery

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultPath    = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

type Registery struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

func New(timeout time.Duration) *Registery {
	return &Registery{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultRegistry = New(defaultTimeout)

func (r *Registery) putServers(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

func (r *Registery) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		// 所有可用的服务节点
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

func (r *Registery) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case "GET":
		// keep it simple, server is in req.Header
		writer.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// keep it simple, server is in req.Header
		addr := request.Header.Get("X-Geerpc-Server")
		if addr == "" {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServers(addr)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Registery) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registery path: ", registryPath)
}
func HandleHTTP() {
	DefaultRegistry.HandleHTTP(defaultPath)
}

func Heartbeat(registery, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registery, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registery, addr)
		}
	}()
}

func sendHeartbeat(reginstery, addr string) error {
	log.Println(addr, "send heart beat to registery", reginstery)
	httpclient := &http.Client{}
	req, _ := http.NewRequest("POST", reginstery, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpclient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
