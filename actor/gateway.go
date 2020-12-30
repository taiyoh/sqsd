package sqsd

import (
	"context"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"github.com/AsynkronIT/protoactor-go/router"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Gateway fetches and removes jobs from SQS.
type Gateway struct {
	ctx      context.Context
	cancel   context.CancelFunc
	parallel int
	queueURL string
	queue    *sqs.SQS
	timeout  int64
	mu       sync.Mutex
}

// GatewayParameter sets parameter in Gateway.
type GatewayParameter func(*Gateway)

// GatewayParallel sets parallel size in Gateway.
func GatewayParallel(p int) GatewayParameter {
	return func(g *Gateway) {
		g.parallel = p
	}
}

// GatewayVisibilityTimeout sets visibility timeout in Gateway to receive messages from SQS.
func GatewayVisibilityTimeout(d time.Duration) GatewayParameter {
	return func(g *Gateway) {
		g.timeout = int64(d.Seconds())
	}
}

// NewGateway returns Gateway object.
func NewGateway(queue *sqs.SQS, qURL string, fns ...GatewayParameter) *Gateway {
	gw := &Gateway{
		queueURL: qURL,
		queue:    queue,
		parallel: 1,
		timeout:  30, // default SQS settings
	}
	for _, fn := range fns {
		fn(gw)
	}
	return gw
}

// NewFetcherGroup returns parallelized Fetcher properties which is provided as BroadcastGroup.
func (f *Gateway) NewFetcherGroup() *actor.Props {
	return router.NewBroadcastPool(f.parallel).WithFunc(f.receiveForFetcher)
}

// StartGateway is operation message for starting requesting to SQS.
type StartGateway struct {
	Sender *actor.PID
}

// receiveForFetcher receives actor messages.
func (g *Gateway) receiveForFetcher(c actor.Context) {
	switch x := c.Message().(type) {
	case *StartGateway:
		g.mu.Lock()
		if g.cancel != nil {
			g.mu.Unlock()
			return
		}
		g.ctx, g.cancel = context.WithCancel(context.Background())
		g.mu.Unlock()
		sender := x.Sender
		go func() {
			for {
				queues, err := g.fetch(g.ctx)
				if err != nil {
					if e, ok := err.(awserr.Error); ok && e.OrigErr() == context.Canceled {
						return
					}
					logger.Error("failed to fetch from SQS", log.Error(err))
				}
				if l := len(queues); l > 0 {
					logger.Debug("caught messages.", log.Int("length", l))
					for _, msg := range queues {
						_ = c.RequestFuture(sender, &PostQueue{Queue: msg}, -1).Wait()
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	case *actor.Stopping:
		g.mu.Lock()
		g.cancel()
		g.cancel = nil
		g.mu.Unlock()
	}
}

func (f *Gateway) fetch(ctx context.Context) ([]Queue, error) {
	out, err := f.queue.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &f.queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
		VisibilityTimeout:   aws.Int64(f.timeout),
	})
	if err != nil {
		return nil, err
	}
	receivedAt := time.Now().UTC()
	queues := make([]Queue, 0, len(out.Messages))
	for _, msg := range out.Messages {
		queues = append(queues, Queue{
			ID:           *msg.MessageId,
			Payload:      *msg.Body,
			Receipt:      *msg.ReceiptHandle,
			ReceivedAt:   receivedAt,
			ResultStatus: NotRequested,
		})
	}
	return queues, nil
}

// NewRemoverGroup returns parallelized Remover properties which is provided as RoundRobinGroup.
func (r *Gateway) NewRemoverGroup() *actor.Props {
	return router.NewRoundRobinPool(r.parallel).
		WithFunc(r.receiveForRemover).
		WithMailbox(mailbox.Bounded(r.parallel * 100))
}

// RemoveQueuesMessage brings Queue to remove from SQS.
type RemoveQueueMessage struct {
	Sender *actor.PID
	Queue  Queue
}

// RemoveQueueResultMessage is message for deleting message from SQS.
type RemoveQueueResultMessage struct {
	Queue Queue
	Err   error
}

func (g *Gateway) receiveForRemover(c actor.Context) {
	switch x := c.Message().(type) {
	case *RemoveQueueMessage:
		var err error
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = g.queue.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &g.queueURL,
				ReceiptHandle: &x.Queue.Receipt,
			})
			cancel()
			if err == nil {
				logger.Debug("succeeded to remove message.", log.String("message_id", x.Queue.ID))
				c.Send(x.Sender, &RemoveQueueResultMessage{Queue: x.Queue})
				return
			}
			time.Sleep(time.Second)
		}
		c.Send(x.Sender, &RemoveQueueResultMessage{Err: err, Queue: x.Queue})
	}
}
