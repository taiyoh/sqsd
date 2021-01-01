package sqsd

import (
	"context"
	"fmt"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/stretchr/testify/assert"
)

type testInvoker func(context.Context, Message) error

func (f testInvoker) Invoke(ctx context.Context, q Message) error {
	return f(ctx, q)
}

type dummyGatewayActor struct {
}

func (a *dummyGatewayActor) Receive(c actor.Context) {
}

func TestConsumer(t *testing.T) {
	sys := actor.NewActorSystem()

	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)

	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}

	gw := &dummyGatewayActor{}
	gateway := sys.Root.Spawn(actor.PropsFromFunc(gw.Receive))
	consumer := NewConsumer(testInvoker(testInvokerFn), gateway, 1)
	queueActor := sys.Root.Spawn(consumer.NewQueueActorProps())
	monitorActor := sys.Root.Spawn(consumer.NewMonitorActorProps())

	go func() {
		for i := 1; i <= 8; i++ {
			q := Message{
				ID:      fmt.Sprintf("queue_id_%d", i),
				Receipt: fmt.Sprintf("queue_receipt_%d", i),
			}
			sys.Root.RequestFuture(queueActor, &PostQueueMessage{Message: q}, -1).Wait()
		}
	}()

	for i := 1; i <= 8; i++ {
		q := <-rcvCh
		res, err := sys.Root.RequestFuture(monitorActor, &CurrentWorkingsMessages{}, -1).Result()
		assert.NoError(t, err)
		tasks, ok := res.([]*Task)
		assert.True(t, ok)
		assert.Len(t, tasks, 1)
		assert.Equal(t, tasks[0].Id, q.ID)
		nextCh <- struct{}{}
	}
}
