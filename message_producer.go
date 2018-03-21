package sqsd

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MessageProducer struct {
	Resource        *Resource
	Tracker         *QueueTracker
	HandleEmptyFunc func()
	Logger          Logger
	Concurrency     int
}

func NewMessageProducer(resource *Resource, tracker *QueueTracker, concurrency uint) *MessageProducer {
	return &MessageProducer{
		Resource: resource,
		Tracker:  tracker,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
		Logger:      tracker.Logger,
		Concurrency: int(concurrency),
	}
}

func (p *MessageProducer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	p.Logger.Info(fmt.Sprintf("MessageProducer start. concurrency=%d", p.Concurrency))
	syncWait := &sync.WaitGroup{}
	syncWait.Add(p.Concurrency + 1)
	for i := 0; i < p.Concurrency; i++ {
		go p.fetch(ctx, syncWait)
	}
	go func() {
		defer syncWait.Done()
		select {
		case <-ctx.Done():
			p.Logger.Info("context cancel detected. stop MessageProducer...")
			return
		}
	}()
	syncWait.Wait()
	p.Logger.Info("MessageProducer closed.")
}

func (p *MessageProducer) fetch(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.DoHandle(ctx)
		}
	}
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
	queues := []*Queue{}
	for _, msg := range results {
		queues = append(queues, NewQueue(msg))
	}
	for _, queue := range queues {
		p.Tracker.Register(queue)
	}
}
