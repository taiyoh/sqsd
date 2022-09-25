package sqsd

import (
	"context"
	"time"
)

// MonitoringService provides grpc handler for MonitoringService.
type MonitoringService struct {
	UnimplementedMonitoringServiceServer
	worker *worker
}

// NewMonitoringService returns new MonitoringService object.
func NewMonitoringService(consumer *worker) *MonitoringService {
	return &MonitoringService{
		worker: consumer,
	}
}

// CurrentWorkings handles CurrentWorkings grpc request using actor system.
func (s *MonitoringService) CurrentWorkings(ctx context.Context, _ *CurrentWorkingsRequest) (*CurrentWorkingsResponse, error) {
	tasks := s.worker.CurrentWorkings(ctx)
	return &CurrentWorkingsResponse{Tasks: tasks}, nil
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
