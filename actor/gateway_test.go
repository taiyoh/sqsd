package sqsd

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

type testQueueReceiver struct {
	fetcher   *actor.PID
	received  []Queue
	completed chan struct{}
}

func (r *testQueueReceiver) Receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *SuccessRetrieveQueuesMessage:
		r.received = append(r.received, x.Queues...)
		if len(r.received) >= 33 {
			r.completed <- struct{}{}
		}
		c.Respond(struct{}{})
	case *StartGateway:
		x.Sender = c.Self()
		c.Request(r.fetcher, x)
	}
}

func TestFetcherAndRemover(t *testing.T) {
	sys := actor.NewActorSystem()
	resourceName := fmt.Sprintf("fetcher-and-remover-%d", time.Now().UnixNano())
	queue := sqs.New(
		session.Must(session.NewSession(awsConf)),
		aws.NewConfig().WithEndpoint(os.Getenv("SQS_ENDPOINT_URL")))
	queueURL, err := setupSQS(t, queue, resourceName)
	if err != nil {
		panic(err)
	}

	rcv := &testQueueReceiver{completed: make(chan struct{})}
	rcvPID := sys.Root.Spawn(actor.PropsFromFunc(rcv.Receive))

	f := NewFetcher(queue, queueURL, 1)
	fetcher := sys.Root.Spawn(f.NewBroadcastPool())

	rcv.fetcher = fetcher

	for i := 1; i <= 33; i++ {
		body := fmt.Sprintf("msgbody_%d", i)
		_, err := queue.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    &queueURL,
			MessageBody: &body,
		})
		assert.NoError(t, err)
	}

	sys.Root.Send(rcvPID, &StartGateway{})

	<-rcv.completed

	assert.Len(t, rcv.received, 33)

	r := NewRemover(queue, queueURL, 33)
	remover := sys.Root.Spawn(r.NewRoundRobinGroup())

	removeRcv := &removeMessageReceiver{
		size:      len(rcv.received),
		completed: make(chan struct{}),
	}
	removeRcvPID := sys.Root.Spawn(actor.PropsFromFunc(removeRcv.Receive))

	sys.Root.Send(remover, &StartGateway{})
	for _, q := range rcv.received {
		qm := &RemoveQueueMessage{Queue: q, Sender: removeRcvPID}
		sys.Root.Send(remover, qm)
	}

	<-removeRcv.completed

	assert.Equal(t, 33, removeRcv.succeeded)
	assert.Equal(t, 0, removeRcv.failed)
}

type removeMessageReceiver struct {
	size      int
	succeeded int
	failed    int
	completed chan struct{}
}

func (r *removeMessageReceiver) Receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *RemoveQueueResultMessage:
		if x.Err != nil {
			r.failed++
		} else {
			r.succeeded++
		}
	}
	if r.succeeded+r.failed >= r.size {
		r.completed <- struct{}{}
	}
}
