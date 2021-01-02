package sqsd

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/log"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Consumer manages Invoker's invokation from receiving queues.
type Consumer struct {
	mu       sync.RWMutex
	capacity int
	working  map[string]*Task
	stack    chan struct{}
	invoker  Invoker
	gateway  *actor.PID
}

// NewConsumer returns Consumer actor Prop.
func NewConsumer(invoker Invoker, gateway *actor.PID, parallel int) *Consumer {
	return &Consumer{
		invoker:  invoker,
		gateway:  gateway,
		capacity: parallel,
		stack:    make(chan struct{}, parallel),
		working:  make(map[string]*Task),
	}
}

// NewQueueActorProps returns properties for QueueReceiver actor.
func (csm *Consumer) NewQueueActorProps() *actor.Props {
	parallel := cap(csm.stack)
	return actor.PropsFromFunc(csm.queueReceiver).WithMailbox(mailbox.Bounded(parallel + 10))
}

// NewMonitorActorProps returns properties for MonitoringReceiver actor.
func (csm *Consumer) NewMonitorActorProps() *actor.Props {
	return actor.PropsFromFunc(csm.monitoringReceiver)
}

// PostQueueMessage represents message that Consumer receives when posted to queue.
type PostQueueMessage struct {
	Message Message
}

// CurrentWorkingsMessages is message which MonitoringReceiver actor receives.
type CurrentWorkingsMessages struct{}

func (csm *Consumer) monitoringReceiver(c actor.Context) {
	switch c.Message().(type) {
	case *CurrentWorkingsMessages:
		csm.mu.Lock()
		tasks := make([]*Task, 0, len(csm.working))
		for _, tsk := range csm.working {
			tasks = append(tasks, tsk)
		}
		csm.mu.Unlock()
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].StartedAt.AsTime().Before(tasks[j].StartedAt.AsTime())
		})
		c.Respond(tasks)
	}
}

func (csm *Consumer) queueReceiver(c actor.Context) {
	switch x := c.Message().(type) {
	case *PostQueueMessage:
		csm.setup(x.Message)
		msgID := log.String("message_id", x.Message.ID)
		logger.Debug("start invoking.", msgID)
		go func(q Message) {
			defer csm.free(q)
			switch err := csm.invoker.Invoke(context.Background(), q); err {
			case nil:
				logger.Debug("succeeded to invoke.", msgID)
				c.Send(csm.gateway, &RemoveQueueMessage{
					Message: q.ResultSucceeded(),
					Sender:  c.Self(),
				})
			default:
				logger.Error("failed to invoke.", log.Error(err), msgID)
			}
		}(x.Message)
		c.Respond(struct{}{})
	}
}

func (csm *Consumer) setup(q Message) {
	csm.stack <- struct{}{}
	csm.mu.Lock()
	defer csm.mu.Unlock()
	csm.working[q.ID] = &Task{
		Id:        q.ID,
		Receipt:   q.Receipt,
		StartedAt: timestamppb.New(time.Now()),
	}
}

func (csm *Consumer) free(q Message) {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	delete(csm.working, q.ID)
	<-csm.stack
}

type distributor struct {
	mu              sync.RWMutex
	capacity        int
	suspendCapacity int
	resumeCapacity  int
	suspended       bool
	buffer          []Message
}

func (csm *Consumer) NewDistributorActorProps() *actor.Props {
	d := &distributor{
		capacity:        csm.capacity,
		suspendCapacity: csm.capacity * 5,
		resumeCapacity:  csm.capacity * 2,
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
