package sqsd

import (
	"context"
	"sync"
	"time"

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
	fetcherInterval  time.Duration
	waitTime         time.Duration
	queueURL         string
	queue            *sqs.SQS
	timeout          int64
	numberOfMessages int64
	locker           locker.QueueLocker
}

// FetcherParameter sets parameter to fetcher by functional option pattern.
type FetcherParameter func(*fetcher)

// FetcherInterval sets interval duration of receiving queue request to fetcher.
func FetcherInterval(d time.Duration) FetcherParameter {
	return func(f *fetcher) {
		f.fetcherInterval = d
	}
}

// FetcherWaitTime sets WaitTimeSecond of receiving message request.
func FetcherWaitTime(d time.Duration) FetcherParameter {
	return func(f *fetcher) {
		f.waitTime = d
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

// startFetcher starts Fetcher to fetch sqs messages.
func (g *Gateway) startFetcher(ctx context.Context, broker chan Message, fns ...FetcherParameter) {
	f := &fetcher{
		queue:            g.queue,
		queueURL:         g.queueURL,
		timeout:          g.timeout,
		fetcherInterval:  100 * time.Millisecond,
		waitTime:         20 * time.Second,
		numberOfMessages: 10,
		locker:           nooplocker.Get(),
	}
	for _, fn := range fns {
		fn(f)
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &f.queueURL,
		MaxNumberOfMessages: &f.numberOfMessages,
		WaitTimeSeconds:     aws.Int64(int64(f.waitTime.Seconds())),
		VisibilityTimeout:   aws.Int64(f.timeout),
	}

	var wg sync.WaitGroup
	wg.Add(g.parallel)
	for i := 0; i < g.parallel; i++ {
		go f.RunForFetch(ctx, &wg, broker, input)
	}
	wg.Wait()

	close(broker)
}

func (f *fetcher) RunForFetch(ctx context.Context, wg *sync.WaitGroup, broker chan Message, input *sqs.ReceiveMessageInput) {
	defer wg.Done()
	logger := getLogger()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		out, err := f.queue.ReceiveMessageWithContext(ctx, input)
		if err != nil {
			if e, ok := err.(awserr.Error); ok && e.OrigErr() == context.Canceled {
				return
			}
			logger.Error("failed to fetch from SQS", "error", err)
		}
		receivedAt := time.Now().UTC()
		for _, msg := range out.Messages {
			if err := f.locker.Lock(ctx, *msg.MessageId); err != nil {
				if err == locker.ErrQueueExists {
					logger.Warn("received message is duplicated", "message_id", *msg.MessageId)
				} else {
					logger.Error("failed to lock", "error", err)
				}
				continue
			}
			broker <- Message{
				ID:         *msg.MessageId,
				Payload:    *msg.Body,
				Receipt:    *msg.ReceiptHandle,
				ReceivedAt: receivedAt,
			}
		}
		logger.Debug("caught messages.", "length", len(out.Messages))
		time.Sleep(f.fetcherInterval)
	}
}

type remover struct {
	queue    *sqs.SQS
	queueURL string
}

func (r *remover) Remove(ctx context.Context, msg Message) (err error) {
	// in some tests, queue object is empty for nothing to do it.
	if r.queue == nil {
		return nil
	}
	logger := getLogger()
	for i := 0; i < 16; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err = r.queue.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      &r.queueURL,
			ReceiptHandle: &msg.Receipt,
		})
		cancel()
		if err == nil {
			logger.Debug("succeeded to remove message", "message_id", msg.ID)
			return nil
		}
		time.Sleep(time.Second)
	}
	return err
}
