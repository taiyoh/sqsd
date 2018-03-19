package sqsd

import (
	"sort"
	"sync"
)

type QueueTracker struct {
	CurrentWorkings *sync.Map
	JobWorking      bool
	queueChan       chan *Queue
	queueStack      chan struct{}
}

func NewQueueTracker(maxProcCount uint) *QueueTracker {
	procCount := int(maxProcCount)
	return &QueueTracker{
		CurrentWorkings: new(sync.Map),
		JobWorking:      true,
		queueChan:       make(chan *Queue, procCount),
		queueStack:      make(chan struct{}, procCount),
	}
}

func (t *QueueTracker) Register(q *Queue) {
	t.queueStack <- struct{}{} // blocking
	t.CurrentWorkings.Store(q.ID(), q)
	t.queueChan <- q
}

func (t *QueueTracker) Complete(q *Queue) {
	t.CurrentWorkings.Delete(q.ID())
	<-t.queueStack // unblock
}

func (t *QueueTracker) CurrentSummaries() []*QueueSummary {
	currentList := []*QueueSummary{}
	t.CurrentWorkings.Range(func(key, val interface{}) bool {
		currentList = append(currentList, (val.(*Queue)).Summary())
		return true
	})
	sort.Slice(currentList, func(i, j int) bool {
		return currentList[i].ReceivedAt < currentList[j].ReceivedAt
	})
	return currentList
}

func (t *QueueTracker) NextQueue() <-chan *Queue {
	return t.queueChan
}

func (t *QueueTracker) Pause() {
	t.JobWorking = false
}

func (t *QueueTracker) Resume() {
	t.JobWorking = true
}

func (t *QueueTracker) IsWorking() bool {
	return t.JobWorking
}
