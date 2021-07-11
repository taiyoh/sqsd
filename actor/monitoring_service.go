package sqsd

import (
	"context"
	"time"

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
	return &MonitoringService{
		rootCtx:  ctx,
		consumer: consumer,
	}
}

// CurrentWorkings handles CurrentWorkings grpc request using actor system.
func (s *MonitoringService) CurrentWorkings(context.Context, *CurrentWorkingsRequest) (*CurrentWorkingsResponse, error) {
	res, err := s.rootCtx.RequestFuture(s.consumer, &CurrentWorkingsMessages{}, -1).Result()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to fetch: %v", err)
	}
	return &CurrentWorkingsResponse{Tasks: res.([]*Task)}, nil
}

// WaitUntilAllEnds waits until all worker tasks finishes.
func (s *MonitoringService) WaitUntilAllEnds(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := s.CurrentWorkings(ctx, nil)
		if err != nil {
			return err
		}
		if len(resp.Tasks) == 0 {
			return nil
		}
		time.Sleep(time.Second)
	}
}
