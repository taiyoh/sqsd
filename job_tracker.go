package sqsd

import (
	"sort"
	"sync"
)

type JobTracker struct {
	CurrentWorkings *sync.Map
	JobWorking      bool
	queueChan       chan *Queue
	queueStack      chan struct{}
}

func NewJobTracker(maxProcCount uint) *JobTracker {
	procCount := int(maxProcCount)
	return &JobTracker{
		CurrentWorkings: new(sync.Map),
		JobWorking:      true,
		queueChan:       make(chan *Queue, procCount),
		queueStack:      make(chan struct{}, procCount),
	}
}

func (t *JobTracker) Register(q *Queue) {
	t.queueStack <- struct{}{} // blocking
	t.CurrentWorkings.Store(q.ID(), q)
	t.queueChan <- q
}

func (t *JobTracker) Complete(q *Queue) {
	t.CurrentWorkings.Delete(q.ID())
	<-t.queueStack // unblock
}

func (t *JobTracker) CurrentSummaries() []*QueueSummary {
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

func (t *JobTracker) NextQueue() <-chan *Queue {
	return t.queueChan
}

func (t *JobTracker) Pause() {
	t.JobWorking = false
}

func (t *JobTracker) Resume() {
	t.JobWorking = true
}

func (t *JobTracker) IsWorking() bool {
	return t.JobWorking
}
