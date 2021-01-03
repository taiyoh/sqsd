package sqsd

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
)

type testDistributorActor struct {
	mu       sync.Mutex
	status   distributorStatus
	captured []Message
	size     int
	ch       chan struct{}
}

func (d *testDistributorActor) change(st distributorStatus) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status = st
}

func (d *testDistributorActor) Receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *postQueueMessages:
		d.mu.Lock()
		d.captured = append(d.captured, x.Messages...)
		d.mu.Unlock()
		if len(d.captured) >= d.size {
			d.ch <- struct{}{}
		}
	case *distributorCurrentStatus:
		c.Respond(d.status)
	}
}

func TestFetcherAndRemover(t *testing.T) {
	sys := actor.NewActorSystem()
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

	g := NewGateway(queue, queueURL, GatewayParallel(5))
	remover := sys.Root.Spawn(g.NewRemoverGroup())

	dist := &testDistributorActor{
		size: 20,
		ch:   make(chan struct{}, 1),
	}
	d := sys.Root.Spawn(actor.PropsFromFunc(dist.Receive))

	f := sys.Root.Spawn(g.NewFetcherGroup(
		d,
		FetcherDistributorInterval(30*time.Millisecond),
		FetcherInterval(50*time.Millisecond),
	))

	<-dist.ch
	dist.change(distributorSuspended)
	time.Sleep(50 * time.Millisecond)

	dist.change(distributorRunning)
	time.Sleep(50 * time.Millisecond)

	sys.Root.Stop(f)

	removeRcv := &removeMessageReceiver{
		size:      20,
		completed: make(chan struct{}),
	}
	removeRcvPID := sys.Root.Spawn(actor.PropsFromFunc(removeRcv.Receive))

	for _, msg := range dist.captured {
		sys.Root.Send(remover, &RemoveQueueMessage{
			Message: msg,
			Sender:  removeRcvPID,
		})
	}

	<-removeRcv.completed

	assert.Equal(t, 20, removeRcv.succeeded)
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
