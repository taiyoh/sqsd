package sqsd

import (
	"context"
	"log"
	"sync"
)

type JobHandler struct {
	Tracker          *JobTracker
	Resource         *Resource
	OnHandleJobEnds  func(res *HandleJobResponse)
	OnHandleJobStart func(job *Job)
}

type HandleJobResponse struct {
	JobID string
	Ok    bool
	Err   error
}

func NewJobHandler(resource *Resource, tracker *JobTracker) *JobHandler {
	return &JobHandler{
		Tracker:          tracker,
		Resource:         resource,
		OnHandleJobStart: func(job *Job) {},
		OnHandleJobEnds:  func(res *HandleJobResponse) {},
	}
}

func (h *JobHandler) RunEventListener(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	syncWait := new(sync.WaitGroup)
	for {
		select {
		case <-ctx.Done():
			syncWait.Wait()
			return
		case job := <-h.Tracker.NextJob():
			syncWait.Add(1)
			go h.HandleJob(ctx, job, syncWait)
		}
	}
}

func (h *JobHandler) HandleJob(ctx context.Context, job *Job, wg *sync.WaitGroup) {
	defer wg.Done()
	h.OnHandleJobStart(job)
	log.Printf("job[%s] HandleJob start.\n", job.ID())
	ok, err := job.Run(ctx)
	if err != nil {
		log.Printf("job[%s] HandleJob request error: %s\n", job.ID(), err)
	}
	if ok {
		h.Resource.DeleteMessage(job.Msg)
	}
	h.Tracker.Complete(job)
	log.Printf("job[%s] HandleJob finished.\n", job.ID())
	h.OnHandleJobEnds(&HandleJobResponse{
		JobID: job.ID(),
		Ok:    ok,
		Err:   err,
	})
}
