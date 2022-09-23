package sqsd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/stretchr/testify/assert"
)

type testInvoker func(context.Context, Message) error

func (f testInvoker) Invoke(ctx context.Context, q Message) error {
	return f(ctx, q)
}

func TestDistributor(t *testing.T) {
	sys := actor.NewActorSystem()
	consumer := &Consumer{
		Capacity: 3,
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

type testDummyRemover struct {
	mu      sync.RWMutex
	removed int
}

func (r *testDummyRemover) Receive(c actor.Context) {
	switch c.Message().(type) {
	case *removeQueueMessage:
		r.mu.Lock()
		r.removed++
		r.mu.Unlock()
	}
}

func (r *testDummyRemover) RemovedCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.removed
}

func TestWorker(t *testing.T) {
	sys := actor.NewActorSystem()
	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)
	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}
	consumer := &Consumer{
		Capacity: 3,
		Invoker:  testInvoker(testInvokerFn),
	}

	d := sys.Root.Spawn(consumer.NewDistributorActorProps())
	ra := &testDummyRemover{}
	r := sys.Root.Spawn(actor.PropsFromFunc(ra.Receive))
	w := sys.Root.Spawn(consumer.NewWorkerActorProps(d, r))
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

	assert.Equal(t, 3, ra.RemovedCount())

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

	assert.Equal(t, 6, ra.RemovedCount())

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

	assert.Equal(t, 9, ra.RemovedCount())

	res, err = sys.Root.RequestFuture(w, &CurrentWorkingsMessages{}, -1).Result()
	assert.NoError(t, err)
	tasks, ok = res.([]*Task)
	assert.True(t, ok)
	assert.Len(t, tasks, 1)
	assert.Equal(t, "id:10", tasks[0].Id)
	nextCh <- struct{}{}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 10, ra.RemovedCount())

	sys.Root.Stop(w)
}
