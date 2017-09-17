package sqsd

import (
	"strconv"
	"strings"
	"net"
	"time"
	"sync"
	"context"
	"testing"
)

func TestStatServer(t *testing.T) {
	tr := NewJobTracker(5)
	h := NewStatHandler(tr)
	c := &SQSDConf{}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	hostport := strings.Split(l.Addr().String(), ":")
	port, _ := strconv.Atoi(hostport[1])
	c.Stat.Port = port

	s := NewStatServer(h, c)
	if s == nil {
		t.Error("stat server not loaded")
	}
	if s.Port != c.Stat.Port {
		t.Error("port invalid")
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run(ctx, wg)
	}()

	time.Sleep(50 * time.Millisecond)

	cancel()
	wg.Wait()
}