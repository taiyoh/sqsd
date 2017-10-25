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

func (t *JobTracker) HasWaitings() bool {
	return len(t.Waitings) > 0
}

func (t *JobTracker) GetAndClearWaitings() []*Job {
	t.mu.Lock()
	jobs := t.Waitings
	t.Waitings = []*Job{}
	t.mu.Unlock()
	return jobs
}

func (t *JobTracker) Add(job *Job) bool {
	addedToWorkings := false
	t.mu.Lock()
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		t.Waitings = append(t.Waitings, job)
	} else {
		t.CurrentWorkings[job.ID()] = job
		addedToWorkings = true
	}
	t.mu.Unlock()
	return addedToWorkings
}

func (t *JobTracker) Delete(job *Job) {
	t.mu.Lock()
	delete(t.CurrentWorkings, job.ID())
	t.mu.Unlock()
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
