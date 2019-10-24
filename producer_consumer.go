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

	return runErrGroup(ctx, func(ctx context.Context, eg *errgroup.Group) {
		eg.Go(func() error { return msgConsumer.Run(ctx) })
		eg.Go(func() error { return msgProducer.Run(ctx) })
	})
}

func runErrGroup(ctx context.Context, fn func(context.Context, *errgroup.Group)) error {
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		<-ctx.Done()
		return context.Canceled
	})
	fn(egCtx, eg)
	if err := eg.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}
