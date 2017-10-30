package sqsd

import (
	"sync"
)

type JobTracker struct {
	CurrentWorkings map[string]*Job
	MaxProcessCount int
	JobWorking      bool
	mu              *sync.RWMutex
	Waitings        []*Job
	nextJobChan     chan *Job
}

func NewJobTracker(maxProcCount uint) *JobTracker {
	return &JobTracker{
		CurrentWorkings: make(map[string]*Job),
		MaxProcessCount: int(maxProcCount),
		JobWorking:      true,
		mu:              &sync.RWMutex{},
		Waitings:        []*Job{},
	}
}

func (t *JobTracker) Add(job *Job) bool {
	var registeredWorkings bool
	t.mu.Lock()
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		t.Waitings = append(t.Waitings, job)
		registeredWorkings = false
	} else {
		t.CurrentWorkings[job.ID()] = job
		registeredWorkings = true
	}
	t.mu.Unlock()
	if registeredWorkings && t.nextJobChan != nil {
		t.nextJobChan <- job
	}
	return registeredWorkings
}

func (t *JobTracker) NextJob() <-chan *Job {
	t.mu.Lock()
	if t.nextJobChan == nil {
		t.nextJobChan = make(chan *Job)
	}
	t.mu.Unlock()
	n := t.nextJobChan
	return n
}

func (t *JobTracker) DeleteAndNextJob(job *Job) {
	var waitingJob *Job
	t.mu.Lock()
	delete(t.CurrentWorkings, job.ID())
	if len(t.Waitings) > 0 {
		waitingJob = t.Waitings[0]
		t.Waitings = t.Waitings[1:]
	}
	t.mu.Unlock()
	if waitingJob != nil {
		t.Add(waitingJob)
	}
}

func (t *JobTracker) CurrentSummaries() []*JobSummary {
	currentList := []*JobSummary{}
	t.mu.RLock()
	for _, job := range t.CurrentWorkings {
		currentList = append(currentList, job.Summary())
	}
	t.mu.RUnlock()
	return currentList
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

func (t *JobTracker) Acceptable() bool {
	if !t.JobWorking {
		return false
	}
	t.mu.RLock()
	l := len(t.CurrentWorkings)
	t.mu.RUnlock()
	if l >= t.MaxProcessCount {
		return false
	}
	return true
}
