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

// SystemConfig represents actor properties and core behavior.
type SystemConfig struct {
	QueueURL          string
	FetcherParallel   int
	InvokerParallel   int
	VisibilityTimeout time.Duration
	MonitoringPort    int
}

// NewSystem returns System object.
func NewSystem(queue *sqs.SQS, invoker Invoker, config SystemConfig) *System {
	as := actor.NewActorSystem()

	gw := NewGateway(queue, config.QueueURL,
		GatewayParallel(config.FetcherParallel),
		GatewayVisibilityTimeout(config.VisibilityTimeout))

	c := NewConsumer(invoker, config.InvokerParallel)

	return &System{
		system:   as,
		gateway:  gw,
		consumer: c,
		port:     config.MonitoringPort,
	}
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
