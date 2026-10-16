package main

import (
	"net"
	"log"
	"io"
	"sync/atomic"
	"time"
	"sync"
	"syscall"
	"context"
	"os/signal"
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

func (b *Balancer) healthCheck(ctx context.Context){
	ticker := time.NewTicker(5*time.Second)
	defer ticker.Stop()

	for {
		select {
		case <- ctx.Done():
			return
		case <- ticker.C:
			for _, v := range b.backs{
				conn, err := newDial("tcp", v.URL)
				if err != nil{
					v.Alive.Store(false)
					continue
				}
				conn.Close()
				v.Alive.Store(true)
			}
		}
	}
}

func main(){
	bal := NewBalancer(Backend)

	listener, err := net.Listen("tcp", addr)
	if err != nil{
		log.Fatal(err)
	}
	log.Printf("server is listening on %v", listener.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var wg sync.WaitGroup
	servClosed := make(chan struct{})

	go bal.healthCheck(ctx)

	go func(){
		for{
			conn, err := listener.Accept()
			if err != nil{
				select{
				case <- servClosed:
					return
				default:
					log.Print(err)
					continue
				}
			}

			wg.Add(1)
			go handleFunc(conn, bal, &wg)
		}
	}()

	<- ctx.Done()

	log.Print("closing...")
	close(servClosed)
	listener.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
        wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        log.Println("All connections are closed")
    case <-shutdownCtx.Done():
        log.Println("Timeout. Forced closing")
    }
}

func newDial(network, addres string) (net.Conn, error){
	d := net.Dialer{
		Timeout: 15*time.Second,
		KeepAlive: 30*time.Second,
	}

	return d.Dial(network, addres)
}

func handleFunc(clientConn net.Conn, balancer *Balancer, wg *sync.WaitGroup){
	defer wg.Done()
	defer clientConn.Close()

	backend := balancer.LeastConnected()
	if backend == nil{
		log.Print("no healthy backend")
		return
	}

	backConn, err := newDial("tcp", backend.URL)
	if err != nil{
		log.Printf("Handle connections: [err] connecting to %s: %v", backend.URL, err)
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