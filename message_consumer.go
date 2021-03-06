package sqsd

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// MessageConsumer provides receiving queues from tracker, requesting queue to worker, and deleting queue from SQS.
type MessageConsumer struct {
	tracker          *QueueTracker
	resource         *Resource
	invoker          WorkerInvoker
	onHandleJobEnds  func(jobID string, err error)
	onHandleJobStart func(q Queue)
	logger           Logger
}

// MessageConsumerHandleFn represents hooks in job start and end.
type MessageConsumerHandleFn func(*MessageConsumer)

// OnHandleJobStartFn represents setter function at job start hook.
func OnHandleJobStartFn(fn func(q Queue)) MessageConsumerHandleFn {
	return func(c *MessageConsumer) {
		c.onHandleJobStart = fn
	}
}

// OnHandleJobEndFn represents setter function at job end hook.
func OnHandleJobEndFn(fn func(jobID string, err error)) MessageConsumerHandleFn {
	return func(c *MessageConsumer) {
		c.onHandleJobEnds = fn
	}
}

// NewMessageConsumer returns MessageConsumer object
func NewMessageConsumer(resource *Resource, tracker *QueueTracker, invoker WorkerInvoker, fns ...MessageConsumerHandleFn) *MessageConsumer {
	c := &MessageConsumer{
		tracker:          tracker,
		resource:         resource,
		invoker:          invoker,
		onHandleJobStart: func(q Queue) {},
		onHandleJobEnds:  func(jobID string, err error) {},
		logger:           tracker.logger,
	}
	for _, fn := range fns {
		fn(c)
	}
	return c
}

func (c *MessageConsumer) nextQueueLoop(ctx context.Context, eg *errgroup.Group) {
	var mu sync.Mutex
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-c.tracker.NextQueue():
			mu.Lock()
			eg.Go(func() error {
				c.HandleJob(ctx, q)
				return nil
			})
			mu.Unlock()
		}
	}
}

// Run provides receiving queue and execute HandleJob asyncronously.
func (c *MessageConsumer) Run(ctx context.Context) {
	c.logger.Info("MessageConsumer start.")
	defer c.logger.Info("MessageConsumer closed.")

	wg := &sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case q := <-c.tracker.NextQueue():
			wg.Add(1)
			go func(q Queue) {
				defer wg.Done()
				c.HandleJob(ctx, q)
			}(q)
		}
	}
}

// HandleJob provides sending queue to worker, and deleting queue when worker response is success.
func (c *MessageConsumer) HandleJob(ctx context.Context, q Queue) {
	c.onHandleJobStart(q)
	c.logger.Debugf("job[%s] HandleJob start.", q.ID)
	err := c.invoker.Invoke(ctx, q)
	if err != nil {
		c.logger.Errorf("job[%s] HandleJob request error: %s", q.ID, err)
		q = q.ResultFailed()
	} else {
		c.resource.DeleteMessage(ctx, q.Receipt)
		q = q.ResultSucceeded()
	}
	c.tracker.Complete(q)
	c.logger.Debugf("job[%s] HandleJob finished.", q.ID)
	c.onHandleJobEnds(q.ID, err)
}
