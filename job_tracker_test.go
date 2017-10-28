package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"testing"
	"time"
)

func TestJobTracker(t *testing.T) {
	tracker := NewJobTracker(5)
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	job := &Job{
		Msg: &sqs.Message{
			MessageId: aws.String("foobar"),
			Body:      aws.String("hoge"),
		},
	}
	addedJobs := []*Job{}
	deletedJobSignal := 0
	cancel := make(chan struct{})
	wait := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				return
			case <-tracker.JobDeleted():
				deletedJobSignal++
				wait <- struct{}{}
			case job := <-tracker.JobAdded():
				if job != nil {
					addedJobs = append(addedJobs, job)
				}
				wait <- struct{}{}
			}
		}
	}()
	time.Sleep(5 * time.Millisecond)
	ok := tracker.Add(job)
	if !ok {
		t.Error("job not inserted")
	}
	<-wait
	if len(addedJobs) != 1 {
		t.Errorf("add event not comes: %d\n", len(addedJobs))
	}
	if _, exists := tracker.CurrentWorkings[job.ID()]; !exists {
		t.Error("job not registered")
	}
	if deletedJobSignal != 0 {
		t.Error("job deleted event comes")
	}
	tracker.Delete(job)
	<-wait
	if _, exists := tracker.CurrentWorkings[job.ID()]; exists {
		t.Error("job not deleted")
	}
	if deletedJobSignal != 1 {
		t.Error("job deleted event not comes")
	}

	addedJobs = []*Job{} // clear

	for i := 0; i < tracker.MaxProcessCount; i++ {
		j := &Job{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tracker.Add(j)
		<-wait
	}

	if len(tracker.Waitings) != 0 {
		t.Error("waiting jobs exists")
	}
	if len(addedJobs) != tracker.MaxProcessCount {
		t.Errorf("job event count is wrong: %d\n", len(addedJobs))
	}

	untrackedJob := &Job{
		Msg: &sqs.Message{
			MessageId: aws.String("id:6"),
			Body:      aws.String("foobar"),
		},
	}
	if ok := tracker.Add(untrackedJob); ok {
		t.Error("job register success...")
	}
	<-wait
	if _, exists := tracker.CurrentWorkings[untrackedJob.ID()]; exists {
		t.Error("job registered ...")
	}

	if len(addedJobs) != tracker.MaxProcessCount {
		t.Errorf("job event count is wrong: %d\n", len(addedJobs))
	}

	if len(tracker.Waitings) != 1 {
		t.Error("waiting jobs not exists")
	}

	if tracker.Acceptable() {
		t.Error("CurrentWorkings is filled but Acceptable() is invalid")
	}

	waitingJob := tracker.ShiftWaitingJobs()
	if waitingJob.ID() != untrackedJob.ID() {
		t.Error("wrong job received")
	}
	if len(tracker.Waitings) != 0 {
		t.Error("waiting job stil exists")
	}

	cancel <- struct{}{}
}

func TestJobWorking(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := NewJobTracker(5)

	if !tr.JobWorking {
		t.Error("JobWorking false")
	}
	if !tr.Acceptable() {
		t.Error("Acceptable() invalid")
	}

	tr.Pause()
	if tr.JobWorking {
		t.Error("JobWorking not changed to true")
	}
	if tr.Acceptable() {
		t.Error("Acceptable() invalid")
	}

	tr.Resume()
	if !tr.JobWorking {
		t.Error("JobWorking not changed to false")
	}
}

func TestCurrentSummaries(t *testing.T) {
	tr := NewJobTracker(5)
	conf := &WorkerConf{
		JobURL: "http://example.com/foo/bar",
	}
	for i := 1; i <= 2; i++ {
		iStr := strconv.Itoa(i)
		msg := &sqs.Message{
			MessageId:     aws.String("foo" + iStr),
			Body:          aws.String("bar" + iStr),
			ReceiptHandle: aws.String("baz" + iStr),
		}
		tr.Add(NewJob(msg, conf))
	}

	summaries := tr.CurrentSummaries()
	for _, summary := range summaries {
		job, exists := tr.CurrentWorkings[summary.ID]
		if !exists {
			t.Errorf("job not found: %s", summary.ID)
		}
		if summary.Payload != *job.Msg.Body {
			t.Errorf("job payload is wrong: %s", summary.Payload)
		}
	}
}
