package sqsd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestStatServer(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	addr := l.Addr().String()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Printf("port error: %s\n", addr)
	}
	tr := NewJobTracker(3)
	port, _ := strconv.Atoi(p)

	time.Sleep(500 * time.Millisecond)

	s := NewStatServer(tr, port)
	if s == nil {
		t.Error("stat server not loaded")
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go s.Run(ctx, wg)

	time.Sleep(50 * time.Millisecond)

	cancel()
	wg.Wait()
}
