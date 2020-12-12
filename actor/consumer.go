package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
)

// Task has current working job.
type Task struct {
	id        string
	receipt   string
	startedAt time.Time
}

// Consumer manages Invoker's invokation from receiving queues.
type Consumer struct {
	mu      sync.RWMutex
	working map[string]*Task
	stack   chan struct{}
	invoker Invoker
	gateway *actor.PID
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewConsumer returns Consumer actor Prop.
func NewConsumer(invoker Invoker, gateway *actor.PID, parallel int) *Consumer {
	return &Consumer{
		invoker: invoker,
		gateway: gateway,
		stack:   make(chan struct{}, parallel),
		working: make(map[string]*Task),
	}
}

// NewQueueActor returns property for QueueReceiver actor.
func (csm *Consumer) NewQueueActor() *actor.Props {
	return actor.PropsFromFunc(csm.queueReceiver)
}

// NewMonitorActor returns property for MonitoringReceiver actor.
func (csm *Consumer) NewMonitorActor() *actor.Props {
	return actor.PropsFromFunc(csm.monitoringReceiver)
}

// PostQueue represents message that Consumer receives when Queue posted.
type PostQueue struct {
	Queue Queue
}

func (csm *Consumer) start() {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	csm.ctx, csm.cancel = context.WithCancel(context.Background())
}

func (csm *Consumer) stop() {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	csm.cancel()
	csm.cancel = nil
}

// CurrentWorkingsMessage is message which MonitoringReceiver actor receives.
type CurrentWorkingsMessage struct{}

func (csm *Consumer) monitoringReceiver(c actor.Context) {
	switch c.Message().(type) {
	case *CurrentWorkingsMessage:
		csm.mu.Lock()
		defer csm.mu.Unlock()
		tasks := make([]*Task, 0, len(csm.working))
		for _, tsk := range csm.working {
			tasks = append(tasks, tsk)
		}
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].startedAt.Before(tasks[j].startedAt)
		})
		c.Respond(tasks)
	}
}

func (csm *Consumer) queueReceiver(c actor.Context) {
	switch x := c.Message().(type) {
	case *actor.Started:
		csm.start()
	case *actor.Stopping:
		csm.stop()
	case *PostQueue:
		csm.setup(x.Queue)
		go func(q Queue) {
			defer csm.free(q)
			switch csm.invoker.Invoke(csm.ctx, q) {
			case nil:
				c.Send(csm.gateway, &RemoveQueueMessage{
					Queue:  q,
					Sender: c.Self(),
				})
				return
			default:
				// TODO: logging
				return
			}
		}(x.Queue)
		c.Respond(struct{}{})
	}
}

func (csm *Consumer) setup(q Queue) {
	csm.stack <- struct{}{}
	csm.mu.Lock()
	defer csm.mu.Unlock()
	csm.working[q.ID] = &Task{
		id:        q.ID,
		receipt:   q.Receipt,
		startedAt: time.Now(),
	}
}

func (csm *Consumer) free(q Queue) {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	delete(csm.working, q.ID)
	<-csm.stack
}
