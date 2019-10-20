package sqsd

import (
	"context"
	"sync"
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

// Run provides receiving queue and execute HandleJob asyncronously.
func (c *MessageConsumer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	syncWait := &sync.WaitGroup{}
	c.logger.Info("MessageConsumer start.")
	for {
		select {
		case <-ctx.Done():
			syncWait.Wait()
			c.logger.Info("MessageConsumer closed.")
			return
		case q := <-c.tracker.NextQueue():
			syncWait.Add(1)
			go func() {
				defer syncWait.Done()
				c.HandleJob(ctx, q)
			}()
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
		c.resource.DeleteMessage(q.Receipt)
		q = q.ResultSucceeded()
	}
	c.tracker.Complete(q)
	c.logger.Debugf("job[%s] HandleJob finished.", q.ID)
	c.onHandleJobEnds(q.ID, err)
}
