package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"leexsh/gee/myrpc/config"
	"leexsh/gee/myrpc/rpc/codec"
	"leexsh/gee/myrpc/server"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64
	ServiceMethod string      //
	Args          interface{} // args
	Reply         interface{} // res
	Error         error
	Done          chan *Call // callback
}

func (c *Call) done() {
	c.Done <- c
}

type Client struct {
	cc       codec.Codec // encode and decode
	opt      *config.Option
	sending  sync.Mutex
	mu       sync.Mutex
	header   codec.Header
	seq      uint64
	pending  map[uint64]*Call // un resolve req
	closing  bool             // call close
	shutdown bool             //server has told us to stop
}

var _ io.Closer = (*Client)(nil)

func NewClient(conn net.Conn, opt *config.Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodeType]
	if f == nil {
		err := fmt.Errorf("invalid code type: %s", opt.CodeType)
		log.Println("rpc client: option error: ", err)
		return nil, err
	}
	// send option
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: option error, ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *config.Option) *Client {
	client := &Client{
		seq:     1, // seq start with 1
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// receive response from server
	go client.receive()
	return client
}

type clientResult struct {
	client *Client
	err    error
}
type newClientFunc func(conn net.Conn, opt *config.Option) (client *Client, err error)

func dailTimeout(f newClientFunc, network, address string, opts ...*config.Option) (client *Client, err error) {
	opt, err := parseOption(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectionTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)
	// new client
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{
			client: client, err: err,
		}
	}()

	// return client; without timeout
	if opt.ConnectionTimeout == 0 {
		res := <-ch
		return res.client, res.err
	}
	// deal connection timeout
	select {
	case <-time.After(opt.ConnectionTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectionTimeout)
	case res := <-ch:
		return res.client, res.err
	}
}

// for user
func Dail(network, address string, opts ...*config.Option) (client *Client, err error) {
	return dailTimeout(NewClient, network, address, opts...)
}

var ErrShutDown = errors.New("connection is shut down")

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutDown
	}
	c.closing = true
	return c.cc.Close()
}

func (c *Client) IsAvaliable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closing && !c.shutdown
}

// register method call
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutDown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

// remove method call from pending
func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

// terminate all calls
func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

// send req to server
func (c *Client) Send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()
	// register this call
	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// prepare req header
	c.header.Seq = seq
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Error = ""

	// encode and send req
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (c *Client) Go(serverMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rtc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serverMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.Send(call)
	return call
}

//func (c *Client) CallReq(serverMethod string, args, reply interface{}) error {
//	call := <-c.Go(serverMethod, args, reply, make(chan *Call, 1)).Done
//return call.Error
//}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client:call fail, " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

// deal with response from server
func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		case call == nil:
			// call not exist
			// cancel send or another reason
			err = c.cc.ReadBody(nil)
		case h.Error != "":
			// header error
			call.Error = fmt.Errorf(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			// default
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				fmt.Println(call.Reply)
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	c.terminateCalls(err)
}

func parseOption(opts ...*config.Option) (*config.Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return config.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}

	opt := opts[0]
	opt.MagicNumber = config.DefaultOption.MagicNumber
	if opt.CodeType == "" {
		opt.CodeType = config.DefaultOption.CodeType
	}
	return opt, nil
}

func NewHTTPClient(conn net.Conn, opt *config.Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", server.DefaultRTCPath))
	// 1. successful http response before switch to rpc
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == server.Connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexecpted HTTP response" + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts ...*config.Option) (*Client, error) {
	return dailTimeout(NewHTTPClient, network, address, opts...)
}

func XDial(rpcaddr string, opts ...*config.Option) (*Client, error) {
	parts := strings.Split(rpcaddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format, rpc addr", rpcaddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dail(protocol, addr, opts...)

	}
}
