package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"leexsh/gee/myrpc/config"
	"leexsh/gee/myrpc/rpc/codec"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

//const MagicNumber = 0x3bef5c
//
//type Option struct {
//	MagicNumber       int        // marks rpc request
//	CodeType          codec.Type // client encode way
//	ConnectionTimeout time.Duration
//	HandleTimeout     time.Duration
//}
//
//var DefaultOption = &Option{
//	MagicNumber:       MagicNumber,
//	CodeType:          codec.GobType,
//	ConnectionTimeout: time.Second * 20,
//}

// 一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。
// 但是为了实现上更简单，GeeRPC 客户端固定采用 JSON 编码 Option，后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，
// 服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容。
// | Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
// | <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
//  Option | Header1 | Body1 | Header2 | Body2 | ...

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// | Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
// | <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt config.Option
	// check opt
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error:", err)
		return
	}
	if opt.MagicNumber != config.MagicNumber {
		log.Printf("rpc server: invalid magic number: %x", opt.MagicNumber)
		return
	}

	f := codec.NewCodecFuncMap[opt.CodeType]
	if f == nil {
		log.Printf("rpc server: invalid codec type: %s", opt.CodeType)
		return
	}
	// f(conn) return gob codec
	s.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

func (s *Server) serveCodec(cc codec.Codec, option *config.Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, option.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type Request struct {
	h             *codec.Header
	argv, replayv reflect.Value // args and callback of req
	mtype         *methodType
	svc           *service
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("rpc server: read header error: ", err)
		}
		return nil, err
	}
	return &h, nil
}

func (s *Server) findService(serviceMethod string) (svc *service, mtyp *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, serverMethod := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: cannot find method" + serviceName)
		return
	}
	svc = svci.(*service)
	mtyp = svc.methods[serverMethod]
	if mtyp == nil {
		err = errors.New("rpc server: cannot find method" + serviceMethod)
	}
	return
}

func (s *Server) readRequest(cc codec.Codec) (*Request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &Request{
		h: h,
	}

	req.argv = reflect.New(reflect.TypeOf(""))
	req.svc, req.mtype, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.replayv = req.mtype.newReply()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Printf("rpc server: read argv err: ", err)
		return req, err
	}
	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *Request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replayv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(cc, req.h, req.replayv.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accpet error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

// accpet all req
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method of exported type
//	- two arguments, both of exported type
//	- the second argument is a pointer
//	- one return value, of type error
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: server already defind" + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

const (
	Connected        = "200 Connected to RPC"
	DefaultRTCPath   = "/_myrpc_"
	DefaultDebugPath = "/debug/geerpc"
)

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "CONNECT" {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(writer, "405 must CONNECT\n")
		return
	}
	conn, _, err := writer.(http.Hijacker).Hijack()
	if err != nil {
		log.Println("rpc hijacking, ", request.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")
	s.ServeConn(conn)
}

func (s *Server) handleHTTP() {
	http.Handle(DefaultRTCPath, s)
	http.Handle(DefaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path:", DefaultDebugPath)
}
func HandleHTTP() {
	DefaultServer.handleHTTP()
}
