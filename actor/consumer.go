package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
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

// NewQueueActorProps returns properties for QueueReceiver actor.
func (csm *Consumer) NewQueueActorProps() *actor.Props {
	return actor.PropsFromFunc(csm.queueReceiver)
}

// NewMonitorActorProps returns properties for MonitoringReceiver actor.
func (csm *Consumer) NewMonitorActorProps() *actor.Props {
	return actor.PropsFromFunc(csm.monitoringReceiver)
}

// PostQueue represents message that Consumer receives when Queue posted.
type PostQueue struct {
	Queue Queue
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
	case *PostQueue:
		csm.setup(x.Queue)
		msgID := log.String("message_id", x.Queue.ID)
		logger.Debug("start invoking.", msgID)
		go func(q Queue) {
			defer csm.free(q)
			switch err := csm.invoker.Invoke(context.Background(), q); err {
			case nil:
				logger.Debug("succeeded to invoke.", msgID)
				c.Send(csm.gateway, &RemoveQueueMessage{
					Queue:  q.ResultSucceeded(),
					Sender: c.Self(),
				})
			default:
				logger.Error("failed to invoke.", log.Error(err), msgID)
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
