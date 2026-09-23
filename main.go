package main

import (
	"net"
	"log"
	"io"
	"sync/atomic"
)

const addr = "127.0.0.1:8080"

var Backend = []string{
	"127.0.0.1:8000",
	"127.0.0.1:8001",
	"127.0.0.1:8002",
}

type Backs struct{
	URL string
	Alive atomic.Bool
	ActiveConns int64
}

type Balancer struct{
	backs []*Backs
}

func NewBalancer(urls []string) *Balancer{
	res := make([]*Backs, len(urls))

	for i, v := range urls {
		res[i] = &Backs{URL: v}
		res[i].Alive.Store(true)
	}

	return &Balancer{backs: res}
}

func (b *Balancer) LeastConnected() *Backs{
	var minBack *Backs
	var minConns int64 = -1

	for _, v := range b.backs{
		if !v.Alive.Load() {continue}
		conns := atomic.LoadInt64(&v.ActiveConns)
		if minConns == -1 || conns < minConns{
			minBack = v
			minConns = conns
		}
	}

	return minBack
}

// func (b *Balancer) healthCheck(){

// }

func main(){
	bal := NewBalancer(Backend)

	listener, err := net.Listen("tcp", addr)
	if err != nil{
		log.Fatal(err)
	}
	defer listener.Close()

	for{
		conn, err := listener.Accept()
		if err != nil{
			log.Print(err)
			continue
		}

		go handleFunc(conn, bal)
	}
}

func handleFunc(clientConn net.Conn, balancer *Balancer){
	defer clientConn.Close()

	backend := balancer.LeastConnected()
	if backend == nil{
		log.Print("no healthy backend")
		return
	}

	backConn, err := net.Dial("tcp", backend.URL)
	if err != nil{
		log.Printf("[err] connecting to %s: %v", backend.URL, err)
		backend.Alive.Store(false)
		return
	}
	defer backConn.Close()

	atomic.AddInt64(&backend.ActiveConns, 1)
	defer atomic.AddInt64(&backend.ActiveConns, -1)

	done := make(chan struct{}, 2)

	go func(){
		io.Copy(backConn, clientConn)
		done <- struct{}{}
	}()

	go func(){
		io.Copy(clientConn, backConn)
		done <- struct{}{}
	}()

	<- done

}