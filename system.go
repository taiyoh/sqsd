package sqsd

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// DisableMonitoring makes gRPC server disable to run.
const DisableMonitoring = -1

// System controls actor system of sqsd.
type System struct {
	gateway  *Gateway
	port     int
	capacity int
	invoker  Invoker
}

// SystemBuilder provides constructor for system object requirements.
type SystemBuilder func(*System)

// GatewayBuilder builds gateway for system.
func GatewayBuilder(queue *sqs.Client, queueURL string, parallel int, timeout time.Duration, params ...GatewayParameter) SystemBuilder {
	return func(s *System) {
		s.gateway = NewGateway(queue, queueURL, params...)
	}
}

// ConsumerBuilder builds consumer for system.
func ConsumerBuilder(invoker Invoker, parallel int) SystemBuilder {
	return func(s *System) {
		s.capacity = parallel
		s.invoker = invoker
	}
}

// MonitorBuilder sets monitor server port to system.
func MonitorBuilder(port int) SystemBuilder {
	return func(s *System) {
		s.port = port
	}
}

// NewSystem returns System object.
func NewSystem(builders ...SystemBuilder) *System {
	sys := &System{
		port: DisableMonitoring,
	}
	for _, b := range builders {
		b(sys)
	}
	return sys
}

// Run starts running actors and gRPC server.
func (s *System) Run(ctx context.Context) error {
	msgsCh := make(chan Message, s.capacity)
	worker := startWorker(ctx, s.invoker, msgsCh, s.gateway)

	monitor := NewMonitoringService(worker)

	if s.port >= 0 {
		grpcServer, err := newGRPCServer(monitor, s.port)
		if err != nil {
			return err
		}

		grpcServer.Start()
		defer grpcServer.Stop()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.gateway.start(ctx, msgsCh)
	}()

	<-ctx.Done()
	getLogger().Info("signal caught. stopping worker...")

	if err := monitor.WaitUntilAllEnds(time.Hour); err != nil {
		return err
	}

	wg.Wait()

	return nil
}
