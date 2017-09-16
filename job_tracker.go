package sqsd

type SQSJobTracker struct {
	CurrentWorkings map[string]*SQSJob
	MaxProcessCount int
}

func NewJobTracker(maxProcCount int) *SQSJobTracker {
	return &SQSJobTracker{
		CurrentWorkings: make(map[string]*SQSJob),
		MaxProcessCount: maxProcCount,
	}
}

func (t *SQSJobTracker) Add(job *SQSJob) bool {
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		return false
	}
	t.CurrentWorkings[job.ID()] = job
	return true
}

func (t *SQSJobTracker) Delete(job *SQSJob) {
	delete(t.CurrentWorkings, job.ID())
}

func (t *SQSJobTracker) CurrentSummaries() []*SQSJobSummary {
	currentList := []*SQSJobSummary{}
	for _, job := range t.CurrentWorkings {
		currentList = append(currentList, job.Summary())
	}
	return currentList
}
