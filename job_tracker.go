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

func (t *JobTracker) Add(job *Job) bool {
	t.mu.Lock()
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		t.mu.Unlock()
		return false
	}
	t.CurrentWorkings[job.ID()] = job
	t.mu.Unlock()
	return true
}

func (t *JobTracker) AddToWaitings(j *Job) {
	t.mu.Lock()
	t.Waitings = append(t.Waitings, j)
	t.mu.Unlock()
}

func (t *JobTracker) Delete(job *Job) {
	t.mu.Lock()
	delete(t.CurrentWorkings, job.ID())
	diff := t.MaxProcessCount - len(t.CurrentWorkings)
	t.mu.Unlock()

	if diff > 0 {
		t.mu.Lock()
		for _, j := range t.Waitings[:diff] {
			t.Add(j)
			j.Go <- struct{}{}
		}
		t.Waitings = t.Waitings[diff:]
		t.mu.Unlock()
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
