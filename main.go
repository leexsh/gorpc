package main

import (
	"context"
	"leexsh/gee/myrpc/balance"
	"leexsh/gee/myrpc/registery"
	"leexsh/gee/myrpc/server"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Foo int

type Args struct{ Num1, Num2 int }

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Second * time.Duration(args.Num1))
	*reply = args.Num2 + args.Num1
	return nil
}

func startRegistry(wg *sync.WaitGroup) {
	l, _ := net.Listen("tcp", ":9999")
	registery.HandleHTTP()
	wg.Done()
	_ = http.Serve(l, nil)
}

func startServer(registryAddr string, wg *sync.WaitGroup) {
	var foo Foo
	l, _ := net.Listen("tcp", ":0")
	ser := server.NewServer()
	_ = ser.Register(&foo)
	registery.Heartbeat(registryAddr, "tcp@"+l.Addr().String(), 0)
	wg.Done()
	ser.Accept(l)
}

func foo(xc *balance.XClient, ctx context.Context, typ, serviceMethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "call":
		err = xc.Call(ctx, serviceMethod, args, &reply)
	case "broadcase":
		err = xc.BroadCast(ctx, serviceMethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error %v\n", typ, serviceMethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, serviceMethod, args.Num2, args.Num1, reply)
	}
}

func call(registry string) {
	d := balance.NewGeeRegistrtDiscovery(registry, 0)
	xc := balance.NewXClient(d, balance.RandomSelect, nil)
	defer func() {
		_ = xc.Close()
	}()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{i, i * i})
		}()
	}
	wg.Wait()
}

func broadcast(registry string) {
	d := balance.NewGeeRegistrtDiscovery(registry, 0)
	xc := balance.NewXClient(d, balance.RandomSelect, nil)
	defer func() {
		_ = xc.Close()
	}()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcase", "Foo.Sum", &Args{i, i * i})
			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
			foo(xc, ctx, "broadcase", "Foo.Sleep", &Args{i, i * i})
		}(i)
	}
	wg.Wait()
}

func main() {
	//log.SetFlags(0)

	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	registryAddr := "http://localhost:9999/_geerpc_/registry"
	var wg sync.WaitGroup
	wg.Add(1)
	go startRegistry(&wg)
	wg.Wait()

	time.Sleep(time.Second)
	wg.Add(2)
	go startServer(registryAddr, &wg)
	go startServer(registryAddr, &wg)
	wg.Wait()

	time.Sleep(time.Second)
	call(registryAddr)
	broadcast(registryAddr)
}
