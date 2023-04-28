package sqsd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestFetcherAndRemover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	resourceName := fmt.Sprintf("fetcher-and-remover-%d", time.Now().UnixNano())
	sess := session.Must(session.NewSession(awsConf))
	queue := sqs.New(
		sess,
		aws.NewConfig().WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")))
	queueURL, err := setupSQS(t, queue, resourceName)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 20; i++ {
		body := fmt.Sprintf(`{"foo":"bar","hoge":100,"index":%d}`, i)
		_, err := queue.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    &queueURL,
			MessageBody: &body,
		})
		assert.NoError(t, err)
	}

	consumer := Consumer{Capacity: 3}
	broker := make(chan Message, consumer.Capacity)

	g := NewGateway(queue, queueURL, GatewayParallel(5))
	go g.startFetcher(ctx, broker,
		FetcherInterval(50*time.Millisecond),
	)

	var removed int32
	var wg sync.WaitGroup
	wg.Add(1)
	ch := make(chan Message, consumer.Capacity)
	removeFn := g.newRemover()
	go func() {
		defer wg.Done()
		for msg := range ch {
			if assert.NoError(t, removeFn(ctx, msg)) {
				atomic.AddInt32(&removed, 1)
			}
		}
	}()

	var sent int
	for msg := range broker {
		ch <- msg
		sent++
		if sent == 20 {
			cancel()
			close(ch)
		}
	}

	wg.Wait()

	assert.Equal(t, int32(20), removed)
}
