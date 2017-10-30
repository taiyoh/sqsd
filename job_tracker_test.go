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
	tracker := NewJobTracker(1)
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	job1 := &Job{
		Msg: &sqs.Message{
			MessageId:     aws.String("id:1"),
			Body:          aws.String("hoge"),
			ReceiptHandle: aws.String("foo"),
		},
	}
	job2 := &Job{
		Msg: &sqs.Message{
			MessageId:     aws.String("id:2"),
			Body:          aws.String("fuga"),
			ReceiptHandle: aws.String("bar"),
		},
	}
	addedJobs := []*Job{}
	cancel := make(chan struct{})
	wait := make(chan struct{})
	go func() {
		for {
			select {
			case <-cancel:
				return
			case job := <-tracker.NextJob():
				if job != nil {
					addedJobs = append(addedJobs, job)
				}
				wait <- struct{}{}
			}
		}
	}()
	time.Sleep(5 * time.Millisecond)
	ok := tracker.Add(job1)
	if !ok {
		t.Error("job1 not inserted")
	}
	ok = tracker.Add(job2)
	if ok {
		t.Error("job2 inserted")
	}
	<-wait
	if len(addedJobs) != 1 {
		t.Errorf("add event not comes: %d\n", len(addedJobs))
	}
	if _, exists := tracker.CurrentWorkings[job1.ID()]; !exists {
		t.Error("job1 not registered")
	}
	tracker.DeleteAndNextJob(job1)
	<-wait
	if _, exists := tracker.CurrentWorkings[job1.ID()]; exists {
		t.Error("job1 not deleted")
	}
	if len(addedJobs) != 2 {
		t.Errorf("add event not comes: %d\n", len(addedJobs))
	}
	if _, exists := tracker.CurrentWorkings[job2.ID()]; !exists {
		t.Error("job2 not registered")
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
