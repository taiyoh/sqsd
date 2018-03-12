package sqsd

import (
	"context"
	"fmt"
	"sync"
)

type JobHandler struct {
	Tracker          *JobTracker
	Resource         *Resource
	OnHandleJobEnds  func(jobID string, ok bool, err error)
	OnHandleJobStart func(job *Job)
	Logger           Logger
}

func NewJobHandler(resource *Resource, tracker *JobTracker, logger Logger) *JobHandler {
	return &JobHandler{
		Tracker:          tracker,
		Resource:         resource,
		OnHandleJobStart: func(job *Job) {},
		OnHandleJobEnds:  func(jobID string, ok bool, err error) {},
		Logger:           logger,
	}
}

func (h *JobHandler) RunEventListener(ctx context.Context) {
	syncWait := new(sync.WaitGroup)
	for {
		select {
		case <-ctx.Done():
			syncWait.Wait()
			return
		case job := <-h.Tracker.NextJob():
			syncWait.Add(1)
			go func() {
				defer syncWait.Done()
				h.HandleJob(ctx, job)
			}()
		}
	}
}

func (h *JobHandler) HandleJob(ctx context.Context, job *Job) {
	h.OnHandleJobStart(job)
	h.Logger.Debug(fmt.Sprintf("job[%s] HandleJob start.\n", job.ID()))
	ok, err := job.Run(ctx)
	if err != nil {
		h.Logger.Error(fmt.Sprintf("job[%s] HandleJob request error: %s\n", job.ID(), err))
	}
	if ok {
		h.Resource.DeleteMessage(job.Msg)
	}
	h.Tracker.Complete(job)
	h.Logger.Debug(fmt.Sprintf("job[%s] HandleJob finished.\n", job.ID()))
	h.OnHandleJobEnds(job.ID(), ok, err)
}
