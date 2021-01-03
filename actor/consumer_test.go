package sqsd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestDistributor(t *testing.T) {
	sys := actor.NewActorSystem()
	consumer := &Consumer{
		capacity: 3,
	}
	d := sys.Root.Spawn(consumer.NewDistributorActorProps())

	t.Run("distributor is running", func(t *testing.T) {
		res, err := sys.Root.RequestFuture(d, &distributorCurrentStatus{}, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, distributorRunning, res)
	})

	t.Run("distributor is suspended", func(t *testing.T) {
		var msgs []Message
		for i := 1; i <= 20; i++ {
			msgs = append(msgs, Message{
				ID: fmt.Sprintf("id:%d", i),
			})
		}
		sys.Root.Send(d, &postQueueMessages{
			Messages: msgs,
		})
		res, err := sys.Root.RequestFuture(d, &distributorCurrentStatus{}, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, distributorSuspended, res)
	})
	t.Run("fetched but suspended", func(t *testing.T) {
		res, err := sys.Root.RequestFuture(d, &fetchQueueMessages{Count: 10}, -1).Result()
		assert.NoError(t, err)
		msgs, ok := res.([]Message)
		assert.True(t, ok)
		assert.Len(t, msgs, 10)
		for i, msg := range msgs {
			id := fmt.Sprintf("id:%d", i+1)
			assert.Equal(t, Message{ID: id}, msg)
		}
		res, err = sys.Root.RequestFuture(d, &distributorCurrentStatus{}, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, distributorSuspended, res)
	})
	t.Run("fetched then running", func(t *testing.T) {
		res, err := sys.Root.RequestFuture(d, &fetchQueueMessages{Count: 5}, -1).Result()
		assert.NoError(t, err)
		msgs, ok := res.([]Message)
		assert.True(t, ok)
		assert.Len(t, msgs, 5)
		for i, msg := range msgs {
			id := fmt.Sprintf("id:%d", i+11)
			assert.Equal(t, Message{ID: id}, msg)
		}
		res, err = sys.Root.RequestFuture(d, &distributorCurrentStatus{}, -1).Result()
		assert.NoError(t, err)
		assert.Equal(t, distributorRunning, res)
	})
	t.Run("fetched over", func(t *testing.T) {
		res, err := sys.Root.RequestFuture(d, &fetchQueueMessages{Count: 100}, -1).Result()
		assert.NoError(t, err)
		msgs, ok := res.([]Message)
		assert.True(t, ok)
		assert.Len(t, msgs, 5)
		for i, msg := range msgs {
			id := fmt.Sprintf("id:%d", i+16)
			assert.Equal(t, Message{ID: id}, msg)
		}
	})
}

func TestWorker(t *testing.T) {
	sys := actor.NewActorSystem()
	consumer := &Consumer{
		capacity: 3,
	}

	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)
	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}
	d := sys.Root.Spawn(consumer.NewDistributorActorProps())
	removed := int64(0)
	r := sys.Root.Spawn(actor.PropsFromFunc(func(c actor.Context) {
		switch c.Message().(type) {
		case *RemoveQueueMessage:
			atomic.AddInt64(&removed, 1)
		}
	}))
	w := sys.Root.Spawn(consumer.NewWorkerActorProps(
		testInvoker(testInvokerFn), d, r,
	))
	msgs := make([]Message, 0, 10)
	for i := 1; i <= 10; i++ {
		msgs = append(msgs, Message{
			ID: fmt.Sprintf("id:%d", i),
		})
	}
	sys.Root.Send(d, &postQueueMessages{Messages: msgs})
	time.Sleep(100 * time.Millisecond)

	res, err := sys.Root.RequestFuture(w, &CurrentWorkingsMessages{}, -1).Result()
	assert.NoError(t, err)
	tasks, ok := res.([]*Task)
	assert.True(t, ok)
	assert.Len(t, tasks, 3)
	sort.Slice(tasks, func(i, j int) bool {
		return strings.Compare(tasks[i].Id, tasks[j].Id) < 0
	})
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("id:%d", i+1)
		assert.Equal(t, id, tasks[i].Id)
		nextCh <- struct{}{}
	}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(3), removed)

	res, err = sys.Root.RequestFuture(w, &CurrentWorkingsMessages{}, -1).Result()
	assert.NoError(t, err)
	tasks, ok = res.([]*Task)
	assert.True(t, ok)
	assert.Len(t, tasks, 3)
	sort.Slice(tasks, func(i, j int) bool {
		return strings.Compare(tasks[i].Id, tasks[j].Id) < 0
	})
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("id:%d", i+4)
		assert.Equal(t, id, tasks[i].Id)
		nextCh <- struct{}{}
	}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(6), removed)

	res, err = sys.Root.RequestFuture(w, &CurrentWorkingsMessages{}, -1).Result()
	assert.NoError(t, err)
	tasks, ok = res.([]*Task)
	assert.True(t, ok)
	assert.Len(t, tasks, 3)
	sort.Slice(tasks, func(i, j int) bool {
		return strings.Compare(tasks[i].Id, tasks[j].Id) < 0
	})
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("id:%d", i+7)
		assert.Equal(t, id, tasks[i].Id)
		nextCh <- struct{}{}
	}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(9), removed)

	res, err = sys.Root.RequestFuture(w, &CurrentWorkingsMessages{}, -1).Result()
	assert.NoError(t, err)
	tasks, ok = res.([]*Task)
	assert.True(t, ok)
	assert.Len(t, tasks, 1)
	assert.Equal(t, "id:10", tasks[0].Id)
	nextCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(10), removed)

	sys.Root.Stop(w)
}
