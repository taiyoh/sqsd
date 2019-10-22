package sqsd

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"golang.org/x/sync/errgroup"
)

func RunProducerAndConsumer(
	ctx context.Context,
	api sqsiface.SQSAPI,
	tracker *QueueTracker,
	invoker WorkerInvoker,
	conf SQSConf) error {
	resource := NewResource(api, conf)
	msgConsumer := NewMessageConsumer(resource, tracker, invoker)
	msgProducer := NewMessageProducer(resource, tracker, conf.Concurrency)
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		msgConsumer.Run(ctx)
		return nil
	})
	eg.Go(func() error {
		msgProducer.Run(ctx)
		return nil
	})

	return eg.Wait()
}
