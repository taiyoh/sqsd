package sqsd

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MessageProducer struct {
	Resource        *Resource
	Tracker         *JobTracker
	Conf            *WorkerConf
	QueueURL        string
	HandleEmptyFunc func()
	Logger          Logger
}

func NewMessageProducer(resource *Resource, tracker *JobTracker, conf *Conf, logger Logger) *MessageProducer {
	return &MessageProducer{
		Resource: resource,
		Tracker:  tracker,
		Conf:     &conf.Worker,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
		Logger: logger,
	}
}

func (p *MessageProducer) Run(ctx context.Context, wg *sync.WaitGroup) {
	p.Logger.Info("MessageProducer start.")
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			p.Logger.Info("context cancelled. stop RunMainLoop.")
			return
		default:
			p.DoHandle(ctx)
		}
	}
	p.Logger.Info("MessageProducer closed.")
}

func (p *MessageProducer) HandleEmpty() {
	p.HandleEmptyFunc()
}

func (p *MessageProducer) DoHandle(ctx context.Context) {
	if !p.Tracker.IsWorking() {
		p.Logger.Debug("tracker not working")
		p.HandleEmpty()
		return
	}
	results, err := p.Resource.GetMessages(ctx)
	if err != nil {
		p.Logger.Error(fmt.Sprintf("GetMessages Error: %s", err))
		p.HandleEmpty()
		return
	}
	if len(results) == 0 {
		p.Logger.Debug("received no messages")
		p.HandleEmpty()
		return
	}
	p.Logger.Debug(fmt.Sprintf("received %d messages. run jobs.\n", len(results)))
	jobs := []*Job{}
	for _, msg := range results {
		jobs = append(jobs, NewJob(msg, p.Conf))
	}
	for _, job := range jobs {
		p.Tracker.Register(job)
	}
}
