package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
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

type distributor struct {
	mu              sync.RWMutex
	capacity        int
	suspendCapacity int
	resumeCapacity  int
	suspended       bool
	buffer          []Message
}

// NewDistributorActorProps returns actor properties of distributor.
func (csm *Consumer) NewDistributorActorProps() *actor.Props {
	d := &distributor{
		capacity:        csm.Capacity,
		suspendCapacity: csm.Capacity * 5,
		resumeCapacity:  csm.Capacity * 2,
	}
	return actor.PropsFromFunc(d.Receive).WithMailbox(mailbox.Bounded(1000))
}

type postQueueMessages struct {
	Messages []Message
}

type fetchQueueMessages struct {
	Count int
}

type distributorCurrentStatus struct{}

type distributorStatus int

const (
	distributorRunning distributorStatus = iota
	distributorSuspended
)

func (d *distributor) Receive(c actor.Context) {
	switch x := c.Message().(type) {
	case *postQueueMessages:
		d.append(x.Messages)
	case *fetchQueueMessages:
		c.Respond(d.fetch(x.Count))
	case *distributorCurrentStatus:
		c.Respond(d.currentStatus())
	}
}

func (d *distributor) append(msgs []Message) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.buffer = append(d.buffer, msgs...)
	if !d.suspended && len(d.buffer) > d.suspendCapacity {
		d.suspended = true
	}
}

func (d *distributor) fetch(count int) []Message {
	var msgs []Message
	d.mu.Lock()
	defer d.mu.Unlock()
	if count <= 0 {
		return msgs
	}
	if len(d.buffer) < count {
		msgs = d.buffer
		d.buffer = nil
	} else {
		msgs = d.buffer[:count]
		d.buffer = d.buffer[count:]
	}
	if d.suspended && len(d.buffer) < d.resumeCapacity {
		d.suspended = false
	}
	return msgs
}

func (d *distributor) currentStatus() distributorStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.suspended {
		return distributorSuspended
	}
	return distributorRunning
}

type worker struct {
	mu            sync.Mutex
	workings      sync.Map
	distributor   *actor.PID
	remover       *actor.PID
	distributorCh chan Message
	removerCh     chan *removeQueueMessage
	invoker       Invoker
	capacity      int
	cancel        context.CancelFunc
}

// NewWorkerActorProps returns actor properties of worker.
func (csm *Consumer) NewWorkerActorProps(distributor, remover *actor.PID) *actor.Props {
	w := &worker{
		capacity:    csm.Capacity,
		invoker:     csm.Invoker,
		distributor: distributor,
		remover:     remover,
	}
	return actor.PropsFromFunc(w.Receive)
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

func (w *worker) Receive(c actor.Context) {
	switch c.Message().(type) {
	case *CurrentWorkingsMessages:
		tasks := make([]*Task, 0, w.capacity)
		w.workings.Range(func(key, val interface{}) bool {
			tasks = append(tasks, val.(*Task))
			return true
		})
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].StartedAt.AsTime().Before(tasks[j].StartedAt.AsTime())
		})
		c.Respond(tasks)
	case *actor.Started:
		ctx, cancel := context.WithCancel(context.Background())
		w.mu.Lock()
		w.cancel = cancel
		w.mu.Unlock()
		for i := 0; i < w.capacity; i++ {
			go w.run(ctx, c)
		}
	case *actor.Stopping:
		w.cancel()
	}
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

func (w *worker) run(ctx context.Context, c actor.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := c.RequestFuture(w.distributor, &fetchQueueMessages{Count: 1}, -1).Result()
			if err != nil {
				logger.Error("failed to fetch messages from distributor.", log.Error(err))
				time.Sleep(time.Second)
				continue
			}
			msgs := res.([]Message)
			if len(msgs) == 0 {
				time.Sleep(time.Second)
				continue
			}
			msg := msgs[0]
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
				c.Send(w.remover, &removeQueueMessage{
					Message: msg.ResultSucceeded(),
					Sender:  c.Self(),
				})
			case locker.ErrQueueExists:
				logger.Warn("received message is duplicated", msgID)
			default:
				logger.Error("failed to invoke.", log.Error(err), msgID)
			}
			w.workings.Delete(msg.ID)
		}
	}
}
