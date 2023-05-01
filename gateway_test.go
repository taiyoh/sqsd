package sqsd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
)

func TestFetcherAndRemover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	resourceName := fmt.Sprintf("fetcher-and-remover-%d", time.Now().UnixNano())
	queue := sqs.NewFromConfig(awsConf, sqsEndpoint)
	queueURL, err := setupSQS(t, queue, resourceName)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 20; i++ {
		body := fmt.Sprintf(`{"foo":"bar","hoge":100,"index":%d}`, i)
		_, err := queue.SendMessage(context.Background(), &sqs.SendMessageInput{
			QueueUrl:    &queueURL,
			MessageBody: &body,
		})
		assert.NoError(t, err)
	}

	broker := make(chan Message, 3)

	f := NewGateway(queue, queueURL, FetchParallel(5), FetchInterval(50*time.Millisecond))
	go f.start(ctx, broker)

	var removed int32
	var wg sync.WaitGroup
	wg.Add(1)
	ch := make(chan Message, 3)
	go func() {
		defer wg.Done()
		for msg := range ch {
			if assert.NoError(t, f.remove(ctx, msg)) {
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
