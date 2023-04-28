package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"golang.org/x/exp/slog"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/taiyoh/sqsd/locker"
)

// Consumer manages Invoker's invokation from receiving queues.
type Consumer struct {
	Capacity int
	Invoker  Invoker
}

type worker struct {
	workings  sync.Map
	broker    chan Message
	remover   remover
	invoker   Invoker
	capacity  int
	semaphore *semaphore.Weighted
}

// startWorker start worker to invoke and remove message.
func (csm *Consumer) startWorker(ctx context.Context, broker chan Message, rm remover) *worker {
	w := &worker{
		capacity:  csm.Capacity,
		invoker:   csm.Invoker,
		broker:    broker,
		remover:   rm,
		semaphore: semaphore.NewWeighted(int64(csm.Capacity)),
	}
	for i := 0; i < w.capacity; i++ {
		go w.RunForProcess(ctx)
	}

	return w
}

// CurrentWorkings returns tasks which are invoked.
func (w *worker) CurrentWorkings(ctx context.Context) []*Task {
	tasks := make([]*Task, 0, w.capacity)
	w.workings.Range(func(key, val interface{}) bool {
		tasks = append(tasks, val.(*Task))
		return true
	})
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].StartedAt.AsTime().Before(tasks[j].StartedAt.AsTime())
	})
	return tasks
}

func (w *worker) wrappedProcess(msg Message) {
	ctx := context.Background()

	// error never be returned because always this method receives new context object.
	_ = w.semaphore.Acquire(ctx, 1)
	defer w.semaphore.Release(1)

	w.workings.Store(msg.ID, &Task{
		Id:        msg.ID,
		Receipt:   msg.Receipt,
		StartedAt: timestamppb.New(time.Now()),
	})
	msgID := slog.Attr{
		Key:   "message_id",
		Value: slog.StringValue(msg.ID),
	}
	logger := getLogger()
	logger.Debug("start to invoke.", msgID)
	switch err := w.invoker.Invoke(ctx, msg); err {
	case nil:
		logger.Debug("succeeded to invoke.", msgID)
		if err := w.remover.Remove(ctx, msg.ResultSucceeded()); err != nil {
			logger.Warn("failed to remove message", "error", err)
		}
	case locker.ErrQueueExists:
		logger.Warn("received message is duplicated", msgID)
	default:
		logger.Error("failed to invoke.", "error", err, msgID)
	}
	w.workings.Delete(msg.ID)
}

func (w *worker) RunForProcess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-w.broker:
			if ok {
				w.wrappedProcess(msg)
			}
		}
	}
}
