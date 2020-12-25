package sqsd

import (
	"context"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/router"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// Fetcher fetches job queues from SQS.
type Fetcher struct {
	ctx      context.Context
	cancel   context.CancelFunc
	parallel int
	queueURL string
	queue    *sqs.SQS
	mu       sync.Mutex
}

// NewFetcher returns Fetcher object.
func NewFetcher(queue *sqs.SQS, qURL string, parallel int) *Fetcher {
	return &Fetcher{
		queueURL: qURL,
		queue:    queue,
		parallel: parallel,
	}
}

// NewFetcherGroup returns parallelized Fetcher properties which is provided as BroadcastGroup.
func (f *Fetcher) NewBroadcastPool() *actor.Props {
	return router.NewBroadcastPool(f.parallel).WithFunc(f.receive)
}

// SuccessRetrieveQueuesMessage brings Queues to message producer.
type SuccessRetrieveQueuesMessage struct {
	Queues []Queue
}

// StartGateway is operation message for starting requesting to SQS.
type StartGateway struct {
	Sender *actor.PID
}

// StopGateway is operation message for stopping requesting to SQS.
type StopGateway struct{}

// receive receives actor messages.
func (f *Fetcher) receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *StartGateway:
		f.mu.Lock()
		if f.cancel != nil {
			f.mu.Unlock()
			return
		}
		f.ctx, f.cancel = context.WithCancel(context.Background())
		f.mu.Unlock()
		sender := x.Sender
		go func() {
			for {
				queues, err := f.fetch(f.ctx)
				if err != nil {
					if err == context.Canceled {
						return
					}
					logger.Error("failed to fetch from SQS", log.Error(err))
				}
				if len(queues) > 0 {
					_ = c.RequestFuture(sender, &SuccessRetrieveQueuesMessage{Queues: queues}, -1).Wait()
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	case *actor.Stopping, *StopGateway:
		f.mu.Lock()
		f.cancel()
		f.cancel = nil
		f.mu.Unlock()
	}
}

func (f *Fetcher) fetch(ctx context.Context) ([]Queue, error) {
	out, err := f.queue.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &f.queueURL,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
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

// Remover removes message from SQS.
type Remover struct {
	queueURL string
	queue    *sqs.SQS
	parallel int
}

// NewRemover reeturns Remover object.
func NewRemover(queue *sqs.SQS, qURL string, parallel int) *Remover {
	return &Remover{
		queueURL: qURL,
		queue:    queue,
		parallel: parallel,
	}
}

// NewRemoverGroup returns parallelized Remover properties which is provided as RoundRobinGroup.
func (r *Remover) NewRoundRobinGroup() *actor.Props {
	return router.NewRoundRobinPool(r.parallel).WithFunc(r.receive)
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

func (r *Remover) receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *RemoveQueueMessage:
		var err error
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = r.queue.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &r.queueURL,
				ReceiptHandle: &x.Queue.Receipt,
			})
			cancel()
			if err == nil {
				c.Send(x.Sender, &RemoveQueueResultMessage{Queue: x.Queue})
				return
			}
		}
		c.Send(x.Sender, &RemoveQueueResultMessage{Err: err, Queue: x.Queue})
	}
}
