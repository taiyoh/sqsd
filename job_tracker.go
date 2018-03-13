package sqsd

import (
	"sort"
	"sync"
)

type JobTracker struct {
	CurrentWorkings *sync.Map
	JobWorking      bool
	jobChan         chan *Queue
	jobStack        chan struct{}
}

func NewJobTracker(maxProcCount uint) *JobTracker {
	procCount := int(maxProcCount)
	return &JobTracker{
		CurrentWorkings: new(sync.Map),
		JobWorking:      true,
		jobChan:         make(chan *Queue, procCount),
		jobStack:        make(chan struct{}, procCount),
	}
}

func (t *JobTracker) Register(q *Queue) {
	t.jobStack <- struct{}{} // blocking
	t.CurrentWorkings.Store(q.ID(), q)
	t.jobChan <- q
}

func (t *JobTracker) Complete(q *Queue) {
	t.CurrentWorkings.Delete(q.ID())
	<-t.jobStack // unblock
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
	return t.jobChan
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
