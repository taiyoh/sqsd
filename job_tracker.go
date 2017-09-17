package sqsd

type SQSJobTracker struct {
	CurrentWorkings map[string]*SQSJob
	MaxProcessCount int
	JobWorking      bool
	pauseChan       chan bool
}

func NewJobTracker(maxProcCount int) *SQSJobTracker {
	return &SQSJobTracker{
		CurrentWorkings: make(map[string]*SQSJob),
		MaxProcessCount: maxProcCount,
		JobWorking:      true,
		pauseChan:       make(chan bool),
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

func (t *SQSJobTracker) Pause() {
	t.JobWorking = false
}

func (t *SQSJobTracker) Resume() {
	t.JobWorking = true
}

func (t *SQSJobTracker) IsWorking() bool {
	return t.JobWorking
}
