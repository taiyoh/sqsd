package sqsd

import (
	"context"
	"sync"
	"time"
)

// MessageProducer provides receiving queues from SQS, and send to tracker
type MessageProducer struct {
	resource        *Resource
	tracker         *QueueTracker
	handleEmptyFunc func()
	logger          Logger
	concurrency     int
}

// NewMessageProducer returns MessageProducer object
func NewMessageProducer(resource *Resource, tracker *QueueTracker, concurrency uint, emptyFuncs ...func()) *MessageProducer {
	emptyFunc := func() {
		time.Sleep(1 * time.Second)
	}
	if len(emptyFuncs) > 0 {
		emptyFunc = emptyFuncs[0]
	}
	return &MessageProducer{
		resource:        resource,
		tracker:         tracker,
		handleEmptyFunc: emptyFunc,
		logger:          tracker.logger,
		concurrency:     int(concurrency),
	}
}

// Run executes DoHandle method asyncronously
func (p *MessageProducer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	p.logger.Infof("MessageProducer start. concurrency=%d", p.concurrency)
	syncWait := &sync.WaitGroup{}
	syncWait.Add(p.concurrency)
	for i := 0; i < p.concurrency; i++ {
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
	p.handleEmptyFunc()
}

// DoHandle receiving queues from SQS, and sending queues to tracker
func (p *MessageProducer) DoHandle(ctx context.Context) {
	if !p.tracker.IsWorking() {
		p.logger.Debug("tracker not working")
		p.HandleEmpty()
		return
	}
	results, err := p.resource.GetMessages(ctx)
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
		p.tracker.Register(NewQueue(msg))
	}
}
