package sqsd

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MessageReceiver struct {
	Resource        *Resource
	Tracker         *JobTracker
	Conf            *WorkerConf
	QueueURL        string
	HandleEmptyFunc func()
	Logger          Logger
}

func NewMessageReceiver(resource *Resource, tracker *JobTracker, conf *Conf, logger Logger) *MessageReceiver {
	return &MessageReceiver{
		Resource: resource,
		Tracker:  tracker,
		Conf:     &conf.Worker,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
		Logger: logger,
	}
}

func (r *MessageReceiver) Run(ctx context.Context, wg *sync.WaitGroup) {
	r.Logger.Debug("MessageReceiver start.")
	defer wg.Done()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				r.Logger.Debug("context cancelled. stop RunMainLoop.")
				return
			default:
				r.DoHandle(ctx)
			}
		}
	}()
	r.Logger.Debug("MessageReceiver closed.")
}

func (r *MessageReceiver) HandleEmpty() {
	r.HandleEmptyFunc()
}

func (r *MessageReceiver) DoHandle(ctx context.Context) {
	if !r.Tracker.IsWorking() {
		r.Logger.Debug("tracker not working")
		r.HandleEmpty()
		return
	}
	results, err := r.Resource.GetMessages(ctx)
	if err != nil {
		r.Logger.Debug(fmt.Sprintf("Error", err))
		r.HandleEmpty()
		return
	}
	if len(results) == 0 {
		r.Logger.Debug("received no messages")
		r.HandleEmpty()
		return
	}
	r.Logger.Debug(fmt.Sprintf("received %d messages. run jobs.\n", len(results)))
	jobs := []*Job{}
	for _, msg := range results {
		jobs = append(jobs, NewJob(msg, r.Conf))
	}
	for _, job := range jobs {
		r.Tracker.Register(job)
	}
}
