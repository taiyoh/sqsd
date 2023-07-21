package sqsd

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/taiyoh/sqsd/locker"
)

// Message provides transition from sqs.Message
type Message struct {
	ID         string
	Payload    string
	Receipt    string
	ReceivedAt time.Time
}

type worker struct {
	workings  sync.Map
	invoker   Invoker
	semaphore *semaphore.Weighted
}

func startWorker(ctx context.Context, ivk Invoker, broker chan Message, rm remover) *worker {
	capacity := cap(broker)
	w := &worker{
		invoker:   ivk,
		semaphore: semaphore.NewWeighted(int64(capacity)),
	}
	for i := 0; i < capacity; i++ {
		go w.RunForProcess(ctx, broker, rm)
	}

	return w
}

type taskList []*Task

func (tasks *taskList) Range(key, val interface{}) bool {
	*tasks = append(*tasks, val.(*Task))
	return true
}

func (tasks *taskList) Slice(i, j int) bool {
	return (*tasks)[i].StartedAt.AsTime().Before((*tasks)[j].StartedAt.AsTime())
}

// CurrentWorkings returns tasks which are invoked.
func (w *worker) CurrentWorkings(ctx context.Context) []*Task {
	var tasks taskList
	w.workings.Range(tasks.Range)
	sort.Slice(tasks, tasks.Slice)
	return tasks
}

type remover interface {
	remove(ctx context.Context, msg Message) error
}

// ErrRetainMessage shows that this message should keep in queue.
// So, this error means that worker must not to remove message.
var ErrRetainMessage = errors.New("this message should be retained")

func (w *worker) wrappedProcess(msg Message, rm remover) {
	ctx := context.Background()

	// error never be returned because always this method receives new context object.
	_ = w.semaphore.Acquire(ctx, 1)
	defer w.semaphore.Release(1)

	w.workings.Store(msg.ID, &Task{
		Id:        msg.ID,
		Receipt:   msg.Receipt,
		StartedAt: timestamppb.New(time.Now()),
	})
	defer w.workings.Delete(msg.ID)

	logger := getLogger().With("message_id", msg.ID)
	logger.Debug("start to invoke.")
	switch err := w.invoker.Invoke(ctx, msg); err {
	case nil:
		logger.Debug("succeeded to invoke.")
		if err := rm.remove(ctx, msg); err != nil {
			logger.Warn("failed to remove message", "error", err)
		}
	case locker.ErrQueueExists:
		logger.Warn("received message is duplicated")
	case ErrRetainMessage:
		logger.Info("received message should be retained")
	default:
		logger.Error("failed to invoke.", "error", err)
	}
}

func (w *worker) RunForProcess(ctx context.Context, broker chan Message, rm remover) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-broker:
			if ok {
				w.wrappedProcess(msg, rm)
			}
		}
	}
}
