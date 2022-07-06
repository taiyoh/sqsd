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

	"github.com/taiyoh/sqsd/locker"
	nooplocker "github.com/taiyoh/sqsd/locker/noop"
)

// Gateway fetches and removes jobs from SQS.
type Gateway struct {
	parallel int
	queueURL string
	queue    *sqs.SQS
	timeout  int64
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

type fetcher struct {
	mu                  sync.Mutex
	cancel              context.CancelFunc
	suspended           bool
	distributorInterval time.Duration
	fetcherInterval     time.Duration
	queueURL            string
	queue               *sqs.SQS
	distributor         *actor.PID
	timeout             int64
	numberOfMessages    int64
	locker              locker.QueueLocker
}

// FetcherParameter sets parameter to fetcher by functional option pattern.
type FetcherParameter func(*fetcher)

// FetcherDistributorInterval sets interval duration of distributor request to fetcher.
// Fetcher watches distributor status because
// fetcher should be stopped when messages which distributor has is over capacity.
func FetcherDistributorInterval(d time.Duration) FetcherParameter {
	return func(f *fetcher) {
		f.distributorInterval = d
	}
}

// FetcherInterval sets interval duration of receiving queue request to fetcher.
func FetcherInterval(d time.Duration) FetcherParameter {
	return func(f *fetcher) {
		f.fetcherInterval = d
	}
}

// FetcherQueueLocker sets QueueLocker in Fetcher to block duplicated queue.
func FetcherQueueLocker(l locker.QueueLocker) FetcherParameter {
	return func(f *fetcher) {
		f.locker = l
	}
}

// FetcherMaxMessages sets MaxNumberOfMessages of SQS between 1 and 10.
// Fetcher's default value is 10.
// if supplied value is out of range, forcely sets 1 or 10.
// (if n is less than 1, set 1 and is more than 10, set 10)
func FetcherMaxMessages(n int64) FetcherParameter {
	if n < 1 {
		n = 1
	}
	if n > 10 {
		n = 10
	}
	return func(f *fetcher) {
		f.numberOfMessages = n
	}
}

// NewFetcherGroup returns parallelized Fetcher properties which is provided as BroadcastGroup.
func (g *Gateway) NewFetcherGroup(distributor *actor.PID, fns ...FetcherParameter) *actor.Props {
	f := &fetcher{
		distributor:         distributor,
		queue:               g.queue,
		queueURL:            g.queueURL,
		timeout:             g.timeout,
		distributorInterval: time.Second,
		fetcherInterval:     100 * time.Millisecond,
		numberOfMessages:    10,
		locker:              nooplocker.Get(),
	}
	for _, fn := range fns {
		fn(f)
	}
	return router.NewBroadcastPool(g.parallel).WithFunc(f.receive)
}

// receive receives actor messages.
func (f *fetcher) receive(c actor.Context) {
	switch c.Message().(type) {
	case *actor.Started:
		ctx, cancel := context.WithCancel(context.Background())
		f.mu.Lock()
		f.cancel = cancel
		f.mu.Unlock()
		go f.watchDistributor(ctx, c)
		go f.run(ctx, c)
	case *actor.Stopping:
		f.cancel()
	}
}

func (f *fetcher) run(ctx context.Context, c actor.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			f.mu.Lock()
			suspended := f.suspended
			f.mu.Unlock()
			if suspended {
				time.Sleep(f.fetcherInterval)
				continue
			}
			messages, err := f.fetch(ctx)
			if err != nil {
				if e, ok := err.(awserr.Error); ok && e.OrigErr() == context.Canceled {
					return
				}
				logger.Error("failed to fetch from SQS", log.Error(err))
			}
			if l := len(messages); l > 0 {
				logger.Debug("caught messages.", log.Int("length", l))
				c.Send(f.distributor, &postQueueMessages{Messages: messages})
			}
			time.Sleep(f.fetcherInterval)
		}
	}
}

func (f *fetcher) watchDistributor(ctx context.Context, c actor.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := c.RequestFuture(f.distributor, &distributorCurrentStatus{}, -1).Result()
			if err != nil {
				logger.Error("failed to get distributor status.", log.Error(err))
				time.Sleep(f.distributorInterval)
				continue
			}
			f.mu.Lock()
			switch status := res.(distributorStatus); {
			case status == distributorSuspended && !f.suspended:
				f.suspended = true
			case status == distributorRunning && f.suspended:
				f.suspended = false
			}
			f.mu.Unlock()
			time.Sleep(f.distributorInterval)
		}
	}
}

func (f *fetcher) fetch(ctx context.Context) ([]Message, error) {
	out, err := f.queue.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &f.queueURL,
		MaxNumberOfMessages: &f.numberOfMessages,
		WaitTimeSeconds:     aws.Int64(20),
		VisibilityTimeout:   aws.Int64(f.timeout),
	})
	if err != nil {
		return nil, err
	}
	receivedAt := time.Now().UTC()
	messages := make([]Message, 0, len(out.Messages))
	for _, msg := range out.Messages {
		if err := f.locker.Lock(ctx, *msg.MessageId); err != nil {
			if err == locker.ErrQueueExists {
				logger.Warn("received message is duplicated", log.String("message_id", *msg.MessageId))
				continue
			}
			return nil, err
		}
		messages = append(messages, Message{
			ID:           *msg.MessageId,
			Payload:      *msg.Body,
			Receipt:      *msg.ReceiptHandle,
			ReceivedAt:   receivedAt,
			ResultStatus: NotRequested,
		})
	}
	return messages, nil
}

type remover struct {
	queue    *sqs.SQS
	queueURL string
	timeout  int64
}

// NewRemoverGroup returns parallelized Remover properties which is provided as RoundRobinGroup.
func (g *Gateway) NewRemoverGroup() *actor.Props {
	r := &remover{
		queue:    g.queue,
		queueURL: g.queueURL,
		timeout:  g.timeout,
	}
	return router.NewRoundRobinPool(g.parallel).
		WithFunc(r.receive).
		WithMailbox(mailbox.Bounded(g.parallel * 100))
}

// removeQueueMessage brings Queue to remove from SQS.
type removeQueueMessage struct {
	Sender  *actor.PID
	Message Message
}

// removeQueueResultMessage is message for deleting message from SQS.
type removeQueueResultMessage struct {
	Queue Message
	Err   error
}

func (r *remover) receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *removeQueueMessage:
		var err error
		for i := 0; i < 16; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = r.queue.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &r.queueURL,
				ReceiptHandle: &x.Message.Receipt,
			})
			cancel()
			if err == nil {
				logger.Debug("succeeded to remove message.", log.String("message_id", x.Message.ID))
				c.Send(x.Sender, &removeQueueResultMessage{Queue: x.Message})
				return
			}
			time.Sleep(time.Second)
		}
		c.Send(x.Sender, &removeQueueResultMessage{Err: err, Queue: x.Message})
	}
}
