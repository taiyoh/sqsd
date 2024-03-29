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

type testInvoker func(context.Context, Message) error

func (f testInvoker) Invoke(ctx context.Context, q Message) error {
	return f(ctx, q)
}

func TestWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rcvCh := make(chan Message, 100)
	nextCh := make(chan struct{}, 100)
	testInvokerFn := func(ctx context.Context, q Message) error {
		rcvCh <- q
		<-nextCh
		return nil
	}

	broker := make(chan Message, 3)
	w := startWorker(ctx, testInvoker(testInvokerFn), broker, &Gateway{})
	msgs := make([]Message, 0, 10)
	for i := 1; i <= 10; i++ {
		msgs = append(msgs, Message{
			ID: fmt.Sprintf("id:%d", i),
		})
	}

	for p, chunk := range [][]Message{
		msgs[0:2],
		msgs[3:5],
		msgs[6:8],
		msgs[9:],
	} {
		for _, msg := range chunk {
			broker <- msg
		}
		time.Sleep(100 * time.Millisecond)

		tasks := w.CurrentWorkings(ctx)
		assert.Len(t, tasks, len(chunk))
		sort.Slice(tasks, func(i, j int) bool {
			return strings.Compare(tasks[i].Id, tasks[j].Id) < 0
		})
		for i := 0; i < len(chunk); i++ {
			id := fmt.Sprintf("id:%d", (i+1)+(p*3))
			assert.Equal(t, id, tasks[i].Id)
			nextCh <- struct{}{}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
