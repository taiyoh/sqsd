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

func (t *JobTracker) Add(job *Job) {
	t.mu.Lock()
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		job.MakeBlocker()
		t.Waitings = append(t.Waitings, job)
	} else {
		t.CurrentWorkings[job.ID()] = job
	}
	t.mu.Unlock()
}

func (t *JobTracker) Delete(job *Job) {
	t.mu.Lock()
	delete(t.CurrentWorkings, job.ID())
	if diff := t.MaxProcessCount - len(t.CurrentWorkings); diff > 0 && len(t.Waitings) > 0 {
		for _, j := range t.Waitings[:diff] {
			j.BreakBlocker()
			t.Add(j)
		}
		t.Waitings = t.Waitings[diff:]
	}
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
