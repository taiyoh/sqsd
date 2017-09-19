package sqsd

type JobTracker struct {
	CurrentWorkings map[string]*SQSJob
	MaxProcessCount int
	JobWorking      bool
	pauseChan       chan bool
}

func NewJobTracker(maxProcCount int) *JobTracker {
	return &JobTracker{
		CurrentWorkings: make(map[string]*SQSJob),
		MaxProcessCount: maxProcCount,
		JobWorking:      true,
		pauseChan:       make(chan bool),
	}
}

func (t *JobTracker) Add(job *SQSJob) bool {
	if len(t.CurrentWorkings) >= t.MaxProcessCount {
		return false
	}
	t.CurrentWorkings[job.ID()] = job
	return true
}

func (t *JobTracker) Delete(job *SQSJob) {
	delete(t.CurrentWorkings, job.ID())
}

func (t *JobTracker) CurrentSummaries() []*SQSJobSummary {
	currentList := []*SQSJobSummary{}
	for _, job := range t.CurrentWorkings {
		currentList = append(currentList, job.Summary())
	}
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
