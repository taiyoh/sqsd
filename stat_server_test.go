package sqsd

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStatServer(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	hostport := strings.Split(l.Addr().String(), ":")
	port, _ := strconv.Atoi(hostport[1])
	tr := NewJobTracker(3)

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
