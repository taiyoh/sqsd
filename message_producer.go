package sqsd

import (
	"context"
	"sync"
	"time"
)

// MessageProducer provides receiving queues from SQS, and send to tracker
type MessageProducer struct {
	Resource        *Resource
	Tracker         *QueueTracker
	HandleEmptyFunc func()
	logger          Logger
	Concurrency     int
}

// NewMessageProducer returns MessageProducer object
func NewMessageProducer(resource *Resource, tracker *QueueTracker, concurrency uint) *MessageProducer {
	return &MessageProducer{
		Resource: resource,
		Tracker:  tracker,
		HandleEmptyFunc: func() {
			time.Sleep(1 * time.Second)
		},
		logger:      tracker.logger,
		Concurrency: int(concurrency),
	}
}

// Run executes DoHandle method asyncronously
func (p *MessageProducer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	p.logger.Infof("MessageProducer start. concurrency=%d", p.Concurrency)
	syncWait := &sync.WaitGroup{}
	syncWait.Add(p.Concurrency)
	for i := 0; i < p.Concurrency; i++ {
		go func() {
			defer syncWait.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					p.DoHandle(ctx)
				}
			}
		}()
	}
	go func() {
		<-ctx.Done()
		p.logger.Info("context cancel detected. stop MessageProducer...")
	}()
	syncWait.Wait()
	p.logger.Info("MessageProducer closed.")
}

// HandleEmpty executes HandleEmptyFunc parameter
func (p *MessageProducer) HandleEmpty() {
	p.HandleEmptyFunc()
}

// DoHandle receiving queues from SQS, and sending queues to tracker
func (p *MessageProducer) DoHandle(ctx context.Context) {
	if !p.Tracker.IsWorking() {
		p.logger.Debug("tracker not working")
		p.HandleEmpty()
		return
	}
	results, err := p.Resource.GetMessages(ctx)
	if err != nil {
		p.logger.Errorf("GetMessages Error: %s", err)
		p.HandleEmpty()
		return
	}
	if len(results) == 0 {
		p.logger.Debug("received no messages")
		p.HandleEmpty()
		return
	}
	p.logger.Debugf("received %d messages. run jobs.\n", len(results))
	for _, msg := range results {
		p.Tracker.Register(NewQueue(msg))
	}
}
