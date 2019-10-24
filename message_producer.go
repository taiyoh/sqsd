package sqsd

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
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
func (p *MessageProducer) Run(ctx context.Context) error {
	c := p.concurrency
	p.logger.Infof("MessageProducer start. concurrency=%d", c)
	defer p.logger.Info("MessageProducer closed.")

	eg, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < c; i++ {
		eg.Go(func() error {
			for p.DoHandle(egCtx) {
				// do handle
			}
			return nil
		})
	}
	eg.Go(func() error {
		<-ctx.Done()
		p.logger.Info("context cancel detected. stop MessageProducer...")
		return context.Canceled
	})
	err := eg.Wait()
	if err == context.Canceled {
		return nil
	}
	return err
}

// HandleEmpty executes HandleEmptyFunc parameter
func (p *MessageProducer) HandleEmpty() {
	p.handleEmptyFunc()
}

// DoHandle receiving queues from SQS, and sending queues to tracker
func (p *MessageProducer) DoHandle(ctx context.Context) bool {
	if !p.tracker.IsWorking() {
		p.logger.Debug("tracker not working")
		p.HandleEmpty()
		return true
	}
	results, err := p.resource.GetMessages(ctx)
	if err != nil {
		if err == context.Canceled {
			return false
		}
		p.logger.Errorf("GetMessages Error: %s", err)
		p.HandleEmpty()
		return true
	}
	if len(results) == 0 {
		p.logger.Debug("received no messages")
		p.HandleEmpty()
		return true
	}
	p.logger.Debugf("received %d messages. run jobs.\n", len(results))
	for _, msg := range results {
		p.tracker.Register(NewQueue(msg))
	}

	return true
}
