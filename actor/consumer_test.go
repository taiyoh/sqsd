package sqsd

import (
	"context"
	"fmt"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/stretchr/testify/assert"
)

type testInvoker func(context.Context, Queue) error

func (f testInvoker) Invoke(ctx context.Context, q Queue) error {
	return f(ctx, q)
}

type dummyGatewayActor struct {
}

func (a *dummyGatewayActor) Receive(c actor.Context) {
}

func TestConsumer(t *testing.T) {
	sys := actor.NewActorSystem()

	rcvCh := make(chan Queue, 100)
	nextCh := make(chan struct{}, 100)

	testInvokerFn := func(ctx context.Context, q Queue) error {
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
			q := Queue{
				ID:      fmt.Sprintf("queue_id_%d", i),
				Receipt: fmt.Sprintf("queue_receipt_%d", i),
			}
			sys.Root.RequestFuture(queueActor, &PostQueue{Queue: q}, -1).Wait()
		}
	}()

	for i := 1; i <= 8; i++ {
		q := <-rcvCh
		res, err := sys.Root.RequestFuture(monitorActor, &CurrentWorkingsMessage{}, -1).Result()
		assert.NoError(t, err)
		tasks, ok := res.([]*Task)
		assert.True(t, ok)
		assert.Len(t, tasks, 1)
		assert.Equal(t, tasks[0].id, q.ID)
		nextCh <- struct{}{}
	}
}
