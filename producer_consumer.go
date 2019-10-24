package sqsd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/sync/errgroup"
)

// RunProducerAndConsumer provides running producer and consumer asyncronously.
func RunProducerAndConsumer(ctx context.Context, tracker *QueueTracker, invoker WorkerInvoker, conf SQSConf) error {
	config := aws.NewConfig().WithRegion(conf.Region).WithEndpoint(conf.URL)
	sess := session.Must(session.NewSession(config))

	resource := NewResource(sqs.New(sess), conf)
	msgConsumer := NewMessageConsumer(resource, tracker, invoker)
	msgProducer := NewMessageProducer(resource, tracker, conf.Concurrency)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return msgConsumer.Run(ctx)
	})
	eg.Go(func() error {
		return msgProducer.Run(ctx)
	})

	return eg.Wait()
}
