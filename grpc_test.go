package sqsd

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPC(t *testing.T) {
	l, err := net.Listen("tcp4", ":0")
	assert.NoError(t, err)
	assert.NotNil(t, l)
	port, err := strconv.Atoi(strings.Split(l.Addr().String(), ":")[1])
	assert.NoError(t, err)
	l.Close()
	sys := NewSystem(
		GatewayBuilder(nil, "", 1, time.Hour),
		ConsumerBuilder(nil, 3),
		MonitorBuilder(port),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgsCh := make(chan Message, 1)
	worker := sys.consumer.startWorker(ctx, msgsCh, remover{})

	monitor := NewMonitoringService(worker)

	grpcServer, err := newGRPCServer(monitor, port)
	assert.NoError(t, err)
	grpcServer.Start()
	defer grpcServer.Stop()

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	assert.NoError(t, err)

	client := NewMonitoringServiceClient(conn)

	resp, err := client.CurrentWorkings(context.Background(), &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
