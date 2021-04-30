package sqsd

import (
	"context"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// DisableMonitoring makes gRPC server disable to run.
const DisableMonitoring = -1

// System controls actor system of sqsd.
type System struct {
	system   *actor.ActorSystem
	gateway  *Gateway
	consumer *Consumer
	port     int
}

// SystemBuilder provides constructor for system object requirements.
type SystemBuilder func(*System)

// GatewayBuilder builds gateway for system.
func GatewayBuilder(queue *sqs.SQS, queueURL string, parallel int, timeout time.Duration) SystemBuilder {
	return func(s *System) {
		s.gateway = NewGateway(queue, queueURL,
			GatewayParallel(parallel),
			GatewayVisibilityTimeout(timeout))
	}
}

// ConsumerBuilder builds consumer for system.
func ConsumerBuilder(invoker Invoker, parallel int) SystemBuilder {
	return func(s *System) {
		s.consumer = NewConsumer(invoker, parallel)
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
		system: actor.NewActorSystem(),
		port:   DisableMonitoring,
	}
	for _, b := range builders {
		b(sys)
	}
	return sys
}

// Run starts running actors and gRPC server.
func (s *System) Run(ctx context.Context) error {
	rCtx := s.system.Root
	distributor := rCtx.Spawn(s.consumer.NewDistributorActorProps())
	remover := rCtx.Spawn(s.gateway.NewRemoverGroup())
	worker := rCtx.Spawn(s.consumer.NewWorkerActorProps(distributor, remover))

	monitor := NewMonitoringService(rCtx, worker)

	if s.port >= 0 {
		grpcServer, err := newGRPCServer(monitor, s.port)
		if err != nil {
			return err
		}

		grpcServer.Start()
		defer grpcServer.Stop()
	}

	fetcher := rCtx.Spawn(s.gateway.NewFetcherGroup(distributor))

	<-ctx.Done()
	logger.Info("signal caught. stopping worker...")

	rCtx.Stop(fetcher)
	rCtx.Stop(distributor)

	if err := monitor.WaitUntilAllEnds(time.Hour); err != nil {
		return err
	}

	rCtx.Poison(remover)

	return nil
}
