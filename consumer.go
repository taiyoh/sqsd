package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/taiyoh/sqsd/locker"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Consumer manages Invoker's invokation from receiving queues.
type Consumer struct {
	Capacity int
	Invoker  Invoker
}

func (csm *Consumer) startDistributor(ctx context.Context) chan Message {
	msgsCh := make(chan Message, csm.Capacity)
	go func() {
		<-ctx.Done()
		close(msgsCh)
	}()
	return msgsCh
}

type messageProcessor func(ctx context.Context, msg Message) error

type worker struct {
	workings      sync.Map
	distributorCh chan Message
	removeFn      messageProcessor
	invoker       Invoker
	capacity      int
}

// startWorker start worker to invoke and remove message.
func (csm *Consumer) startWorker(ctx context.Context, distributor chan Message, removeFn messageProcessor) *worker {
	w := &worker{
		capacity:      csm.Capacity,
		invoker:       csm.Invoker,
		distributorCh: distributor,
		removeFn:      removeFn,
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
	w.workings.Store(msg.ID, &Task{
		Id:        msg.ID,
		Receipt:   msg.Receipt,
		StartedAt: timestamppb.New(time.Now()),
	})
	msgID := NewField("message_id", msg.ID)
	logger.Debug("start to invoke.", msgID)
	ctx := context.Background()
	switch err := w.invoker.Invoke(ctx, msg); err {
	case nil:
		logger.Debug("succeeded to invoke.", msgID)
		if err := w.removeFn(ctx, msg.ResultSucceeded()); err != nil {
			logger.Warn("failed to remove message", NewField("error", err))
		}
	case locker.ErrQueueExists:
		logger.Warn("received message is duplicated", msgID)
	default:
		logger.Error("failed to invoke.", NewField("error", err), msgID)
	}
	w.workings.Delete(msg.ID)
}

func (w *worker) RunForProcess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-w.distributorCh:
			w.wrappedProcess(msg)
		}
	}
}
