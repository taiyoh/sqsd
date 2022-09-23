package sqsd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/stretchr/testify/assert"
)

func TestMonitoringService(t *testing.T) {
	sys := actor.NewActorSystem()
	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)
	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}
	consumer := &Consumer{Invoker: testInvoker(testInvokerFn), Capacity: 3}

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

	monitor := NewMonitoringService(sys.Root, w)
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- monitor.WaitUntilAllEnds(time.Hour)
	}()
	ctx := context.Background()
	resp, err := monitor.CurrentWorkings(ctx, &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	tasks := resp.GetTasks()
	assert.Len(t, tasks, 3)
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		ids = append(ids, tasks[i].GetId())
		nextCh <- struct{}{}
	}
	sort.Slice(ids, func(i, j int) bool {
		return strings.Compare(ids[i], ids[j]) < 0
	})
	time.Sleep(50 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	tasks = resp.GetTasks()
	assert.Len(t, tasks, 3)
	ids2 := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		ids2 = append(ids2, tasks[i].GetId())
		nextCh <- struct{}{}
	}
	sort.Slice(ids2, func(i, j int) bool {
		return strings.Compare(ids2[i], ids2[j]) < 0
	})
	assert.NotEqual(t, ids, ids2)
	time.Sleep(50 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	tasks = resp.GetTasks()
	assert.Len(t, tasks, 3)
	ids3 := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		ids3 = append(ids3, tasks[i].GetId())
		nextCh <- struct{}{}
	}
	assert.NotEqual(t, ids, ids3)
	assert.NotEqual(t, ids2, ids3)
	time.Sleep(50 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, &CurrentWorkingsRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	tasks = resp.GetTasks()
	assert.Len(t, tasks, 1)
	nextCh <- struct{}{}
	assert.NoError(t, <-errCh)

	sys.Root.Stop(w)
}
