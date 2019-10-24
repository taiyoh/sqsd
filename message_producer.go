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

func (p *MessageProducer) runConcurrencyHandle(ctx context.Context, eg *errgroup.Group) {
	for i := 0; i < p.concurrency; i++ {
		eg.Go(func() error {
			for p.DoHandle(ctx) {
				// do handle
			}
			return nil
		})
	}
}

// Run executes DoHandle method asyncronously
func (p *MessageProducer) Run(ctx context.Context) error {
	p.logger.Infof("MessageProducer start. concurrency=%d", p.concurrency)
	defer p.logger.Info("MessageProducer closed.")

	if err := runErrGroup(ctx, p.runConcurrencyHandle); err != nil {
		return err
	}
	return nil
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
