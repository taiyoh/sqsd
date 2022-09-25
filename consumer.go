package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/taiyoh/sqsd/locker"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Consumer manages Invoker's invokation from receiving queues.
type Consumer struct {
	Capacity int
	Invoker  Invoker
}

// CurrentWorkingsMessages is message which MonitoringReceiver actor receives.
type CurrentWorkingsMessages struct{}

func (csm *Consumer) startDistributor(ctx context.Context) (chan Message, chan *removeQueueMessage) {
	msgsCh := make(chan Message, csm.Capacity)
	rmCh := make(chan *removeQueueMessage, csm.Capacity*5)
	go func() {
		<-ctx.Done()
		close(msgsCh)
		close(rmCh)
	}()
	return msgsCh, rmCh
}

type worker struct {
	workings      sync.Map
	distributorCh chan Message
	removerCh     chan *removeQueueMessage
	invoker       Invoker
	capacity      int
}

// startWorker start worker to invoke and remove message.
func (csm *Consumer) startWorker(ctx context.Context, distributor chan Message, remover chan *removeQueueMessage) *worker {
	w := &worker{
		capacity:      csm.Capacity,
		invoker:       csm.Invoker,
		distributorCh: distributor,
		removerCh:     remover,
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

func (w *worker) RunForProcess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-w.distributorCh:
			w.workings.Store(msg.ID, &Task{
				Id:        msg.ID,
				Receipt:   msg.Receipt,
				StartedAt: timestamppb.New(time.Now()),
			})
			msgID := log.String("message_id", msg.ID)
			logger.Debug("start to invoke.", msgID)
			switch err := w.invoker.Invoke(context.Background(), msg); err {
			case nil:
				logger.Debug("succeeded to invoke.", msgID)
				ch := make(chan removeQueueResultMessage)
				w.removerCh <- &removeQueueMessage{
					Message:  msg.ResultSucceeded(),
					SenderCh: ch,
				}
				if resp := <-ch; resp.Err != nil {
					logger.Warn("failed to remove message", log.Error(err))
				}
			case locker.ErrQueueExists:
				logger.Warn("received message is duplicated", msgID)
			default:
				logger.Error("failed to invoke.", log.Error(err), msgID)
			}
			w.workings.Delete(msg.ID)
		}
	}
}
