package sqsd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMonitoringService(t *testing.T) {
	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)
	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := make(chan Message, 3)
	w := startWorker(ctx, testInvoker(testInvokerFn), broker, &Gateway{})
	monitor := NewMonitoringService(w)

	resp, err := monitor.CurrentWorkings(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Empty(t, resp.GetTasks())

	for i := 1; i <= 3; i++ {
		broker <- Message{
			ID: fmt.Sprintf("id:%d", i),
		}
	}

	time.Sleep(100 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, nil)
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
	time.Sleep(100 * time.Millisecond)

	for i := 4; i <= 6; i++ {
		broker <- Message{
			ID: fmt.Sprintf("id:%d", i),
		}
	}

	time.Sleep(100 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, nil)
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

	for i := 7; i <= 9; i++ {
		broker <- Message{
			ID: fmt.Sprintf("id:%d", i),
		}
	}
	time.Sleep(100 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, nil)
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

	broker <- Message{
		ID: "id:10",
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- monitor.WaitUntilAllEnds(time.Hour)
	}()

	time.Sleep(100 * time.Millisecond)

	resp, err = monitor.CurrentWorkings(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	tasks = resp.GetTasks()
	assert.Len(t, tasks, 1)
	nextCh <- struct{}{}
	assert.NoError(t, <-errCh)
}
