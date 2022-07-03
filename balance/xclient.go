package balance

import (
	"context"
	"io"
	client2 "leexsh/gee/myrpc/client"
	"leexsh/gee/myrpc/config"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovry
	mode    SelectMode
	opt     *config.Option
	mu      sync.Mutex
	clients map[string]*client2.Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovry, mode SelectMode, opt *config.Option) *XClient {
	return &XClient{d: d, mode: mode, opt: opt, clients: make(map[string]*client2.Client)}
}

func (X XClient) Close() error {
	X.mu.Lock()
	defer X.mu.Unlock()
	for key, client := range X.clients {
		_ = client.Close()
		delete(X.clients, key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*client2.Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	cli, ok := xc.clients[rpcAddr]
	if ok && !cli.IsAvaliable() {
		_ = cli.Close()
		delete(xc.clients, rpcAddr)
		cli = nil
	}

	if cli == nil {
		var err error
		// todo
		cli, err = client2.XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = cli
	}
	return cli, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	cli, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return cli.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serverMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serverMethod, args, reply)
}

// Broadcast invokes the named function for every server registered in discovery
func (xc *XClient) BroadCast(ctx context.Context, serverMothod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var cloneReply interface{}
			if reply != nil {
				cloneReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serverMothod, args, cloneReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(cloneReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
