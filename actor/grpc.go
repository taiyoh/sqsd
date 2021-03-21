package sqsd

import (
	"fmt"
	"net"
	"sync"

	"github.com/AsynkronIT/protoactor-go/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type grpcServer struct {
	wg       sync.WaitGroup
	server   *grpc.Server
	listener net.Listener
}

func newGRPCServer(srv MonitoringServiceServer, port int) (*grpcServer, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	reflection.Register(server)
	RegisterMonitoringServiceServer(server, srv)

	return &grpcServer{
		server:   server,
		listener: lis,
	}, nil
}

func (s *grpcServer) Start() {
	s.wg.Add(1)
	go func() {
		defer logger.Info("gRPC server closed.")
		defer s.wg.Done()
		logger.Info("gRPC server start.", log.Object("addr", s.listener.Addr()))
		if err := s.server.Serve(s.listener); err != nil && err != grpc.ErrServerStopped {
			logger.Error("failed to stop gRPC server.", log.Error(err))
		}
	}()
}

func (s *grpcServer) Stop() {
	s.server.Stop()
	s.wg.Wait()
}
