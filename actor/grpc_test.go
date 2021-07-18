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

	rCtx := sys.system.Root
	distributor := rCtx.Spawn(sys.consumer.NewDistributorActorProps())
	remover := rCtx.Spawn(sys.gateway.NewRemoverGroup())
	worker := rCtx.Spawn(sys.consumer.NewWorkerActorProps(distributor, remover))

	monitor := NewMonitoringService(rCtx, worker)

	grpcServer, err := newGRPCServer(monitor, port)
	assert.NoError(t, err)
	grpcServer.Start()
	defer grpcServer.Stop()

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)

	assert.NoError(t, err)

	client := NewMonitoringServiceClient(conn)

	resp, err := client.CurrentWorkings(context.Background(), &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
