package sqsd

import (
	"context"
	"fmt"
	"os"
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
	msgsCh, removeCh := consumer.startDistributor(ctx)

	g := NewGateway(queue, queueURL, GatewayParallel(5))
	go g.StartRemover(ctx, removeCh)
	go g.StartFetcher(ctx, msgsCh,
		FetcherDistributorInterval(30*time.Millisecond),
		FetcherInterval(50*time.Millisecond),
	)

	received := make([]Message, 0, 20)
	for msg := range msgsCh {
		received = append(received, msg)
		if len(received) >= 20 {
			break
		}
	}

	rmCh := make(chan removeQueueResultMessage, 20)
	t.Cleanup(func() { close(rmCh) })

	for _, msg := range received {
		removeCh <- &removeQueueMessage{
			Message:  msg,
			SenderCh: rmCh,
		}
	}

	for len(rmCh) < 20 {
	}
}
