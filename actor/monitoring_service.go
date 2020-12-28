package sqsd

import (
	"context"

	"github.com/AsynkronIT/protoactor-go/actor"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// MonitoringService provides grpc handler for MonitoringService.
type MonitoringService struct {
	UnimplementedMonitoringServiceServer
	rootCtx  *actor.RootContext
	consumer *actor.PID
}

// NewMonitoringService returns new MonitoringService object.
func NewMonitoringService(ctx *actor.RootContext, consumer *actor.PID) *MonitoringService {
	svc := &MonitoringService{}
	svc.rootCtx = ctx
	svc.consumer = consumer
	return svc
}

// CurrentWorkings handles CurrentWorkings grpc request using actor system.
func (s *MonitoringService) CurrentWorkings(context.Context, *CurrentWorkingsRequest) (*CurrentWorkingsResponse, error) {
	res, err := s.rootCtx.RequestFuture(s.consumer, &CurrentWorkingsMessage{}, -1).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch: %v", err)
	}
	return &CurrentWorkingsResponse{Tasks: res.([]*Task)}, nil
}
