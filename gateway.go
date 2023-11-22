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
	queueURL        string
	queue           *sqs.SQS
	locker          locker.QueueLocker
	fetcherInterval time.Duration
	parallel        int
	input           *sqs.ReceiveMessageInput
}

type gatewayParams struct {
	fetcherInterval  time.Duration
	waitTime         int64
	timeout          int64
	numberOfMessages int64
	parallel         int
	locker           locker.QueueLocker
}

// NewGateway returns Gateway object.
func NewGateway(queue *sqs.SQS, queueURL string, params ...GatewayParameter) *Gateway {
	param := gatewayParams{
		fetcherInterval:  100 * time.Millisecond,
		timeout:          30, // default Visibility Timeout
		waitTime:         int64((20 * time.Second).Seconds()),
		numberOfMessages: 10,
		parallel:         1,
		locker:           nooplocker.Get(),
	}
	for _, fn := range params {
		fn(&param)
	}

	return &Gateway{
		queue:           queue,
		queueURL:        queueURL,
		fetcherInterval: param.fetcherInterval,
		locker:          nooplocker.Get(),
		parallel:        param.parallel,
		input: &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: &param.numberOfMessages,
			WaitTimeSeconds:     aws.Int64(param.waitTime),
			VisibilityTimeout:   aws.Int64(param.timeout),
		},
	}
}

// GatewayParameter sets parameter to fetcher by functional option pattern.
type GatewayParameter func(*gatewayParams)

// FetchInterval sets interval duration of receiving queue request to fetcher.
func FetchInterval(d time.Duration) GatewayParameter {
	return func(g *gatewayParams) {
		g.fetcherInterval = d
	}
}

// FetcherWaitTime sets WaitTimeSecond of receiving message request.
func FetcherWaitTime(d time.Duration) GatewayParameter {
	return func(g *gatewayParams) {
		g.waitTime = int64(d.Seconds())
	}
}

// FetcherVisibilityTimeout sets VisibilityTimeout of receiving message request.
func FetcherVisibilityTimeout(d time.Duration) GatewayParameter {
	const max = 12 * time.Hour
	if d > max {
		d = max
	}
	return func(g *gatewayParams) {
		g.timeout = int64(d.Seconds())
	}
}

// FetcherQueueLocker sets FetcherQueueLocker in Gateway to block duplicated queue.
func FetcherQueueLocker(l locker.QueueLocker) GatewayParameter {
	return func(g *gatewayParams) {
		g.locker = l
	}
}

// FetcherMaxMessages sets MaxNumberOfMessages of SQS between 1 and 10.
// Fetcher's default value is 10.
// if supplied value is out of range, forcely sets 1 or 10.
// (if n is less than 1, set 1 and is more than 10, set 10)
func FetcherMaxMessages(n int64) GatewayParameter {
	if n < 1 {
		n = 1
	}
	if n > 10 {
		n = 10
	}
	return func(f *gatewayParams) {
		f.numberOfMessages = n
	}
}

// FetcherParalles sets pallalel count of fetching process to SQS.
func FetchParallel(n int) GatewayParameter {
	return func(g *gatewayParams) {
		g.parallel = n
	}
}

func (f Gateway) start(ctx context.Context, broker chan Message) {
	var wg sync.WaitGroup
	wg.Add(f.parallel)
	for i := 0; i < f.parallel; i++ {
		go f.runForFetch(ctx, &wg, broker, f.input)
	}
	wg.Wait()

	close(broker)
}

func (f *Gateway) runForFetch(ctx context.Context, wg *sync.WaitGroup, broker chan Message, input *sqs.ReceiveMessageInput) {
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

// Remove sends delete-message to SQS.
func (g *Gateway) remove(ctx context.Context, msg Message) (err error) {
	// in some tests, queue object is empty for nothing to do it.
	if g.queue == nil {
		return nil
	}
	logger := getLogger()
	for i := 0; i < 16; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err = g.queue.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      &g.queueURL,
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
