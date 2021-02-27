package sqsd

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/aws/aws-sdk-go/service/sqs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type actorsCollection struct {
	distributor *actor.PID
	remover     *actor.PID
	fetcher     *actor.PID
	worker      *actor.PID
}

// System controls actor system of sqsd.
type System struct {
	wg       sync.WaitGroup
	system   *actor.ActorSystem
	gateway  *Gateway
	consumer *Consumer
	grpc     *grpc.Server
	actors   actorsCollection
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

	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)

	return &System{
		system:   as,
		gateway:  gw,
		consumer: c,
		port:     config.MonitoringPort,
		grpc:     grpcServer,
	}
}

// Start starts running actors and gRPC server.
func (s *System) Start() error {
	rCtx := s.system.Root
	distributor := rCtx.Spawn(s.consumer.NewDistributorActorProps())
	remover := rCtx.Spawn(s.gateway.NewRemoverGroup())
	fetcher := rCtx.Spawn(s.gateway.NewFetcherGroup(distributor))
	worker := rCtx.Spawn(s.consumer.NewWorkerActorProps(distributor, remover))

	s.actors = actorsCollection{
		distributor: distributor,
		remover:     remover,
		fetcher:     fetcher,
		worker:      worker,
	}

	RegisterMonitoringServiceServer(s.grpc, NewMonitoringService(rCtx, worker))

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer logger.Info("gRPC server closed.")
		defer s.wg.Done()
		logger.Info("gRPC server start.", log.Object("addr", lis.Addr()))
		if err := s.grpc.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.Error("failed to stop gRPC server.", log.Error(err))
		}
	}()

	return nil
}

// Stop stops actors and gRPC server.
func (s *System) Stop() error {
	rCtx := s.system.Root
	rCtx.Stop(s.actors.fetcher)
	rCtx.Stop(s.actors.distributor)

	msg := &CurrentWorkingsMessages{}
	for {
		res, err := rCtx.RequestFuture(s.actors.worker, msg, -1).Result()
		if err != nil {
			return err
		}
		if tasks := res.([]*Task); len(tasks) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	rCtx.Poison(s.actors.remover)

	s.grpc.Stop()
	s.wg.Wait()

	return nil
}
