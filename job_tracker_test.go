package sqsd

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestJobTracker(t *testing.T) {
	tracker := NewJobTracker(1)
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	mu := new(sync.Mutex)

	now := time.Now()

	job1 := &Job{
		Msg: &sqs.Message{
			MessageId:     aws.String("id:1"),
			Body:          aws.String("hoge"),
			ReceiptHandle: aws.String("foo"),
		},
		ReceivedAt: now.Add(1),
	}
	job2 := &Job{
		Msg: &sqs.Message{
			MessageId:     aws.String("id:2"),
			Body:          aws.String("fuga"),
			ReceiptHandle: aws.String("bar"),
		},
		ReceivedAt: now,
	}

	allJobRegistered := false
	go func() {
		tracker.Register(job1)
		tracker.Register(job2)
		mu.Lock()
		allJobRegistered = true
		mu.Unlock()
	}()

	time.Sleep(5 * time.Millisecond)

	mu.Lock()
	if allJobRegistered {
		t.Error("2 jobs inserted")
	}
	mu.Unlock()

	receivedJob := <-tracker.NextJob()
	if receivedJob.ID() != job1.ID() {
		t.Error("wrong order")
	}

	summaries := tracker.CurrentSummaries()
	if summaries[0].ID != job1.ID() {
		t.Error("wrong order")
	}

	tracker.Complete(receivedJob)

	time.Sleep(10 * time.Microsecond)

	mu.Lock()
	if !allJobRegistered {
		t.Error("second job not registered")
	}
	mu.Unlock()

	receivedJob = <-tracker.NextJob()
	if receivedJob.ID() != job2.ID() {
		t.Error("wrong order")
	}

	summaries = tracker.CurrentSummaries()
	if summaries[0].ID != job2.ID() {
		t.Error("wrong order")
	}
}

func TestJobWorking(t *testing.T) {
	tr := NewJobTracker(5)

	if !tr.JobWorking {
		t.Error("JobWorking false")
	}

	tr.Pause()
	if tr.JobWorking {
		t.Error("JobWorking not changed to true")
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
	now := time.Now()
	for i := 1; i <= 2; i++ {
		iStr := strconv.Itoa(i)
		msg := &sqs.Message{
			MessageId:     aws.String("foo" + iStr),
			Body:          aws.String("bar" + iStr),
			ReceiptHandle: aws.String("baz" + iStr),
		}
		job := NewJob(msg, conf)
		job.ReceivedAt = now.Add(time.Duration(i))
		tr.Register(job)
	}

	summaries := tr.CurrentSummaries()
	for _, summary := range summaries {
		data, exists := tr.CurrentWorkings.Load(summary.ID)
		if !exists {
			t.Errorf("job not found: %s", summary.ID)
		}
		if summary.Payload != *(data.(*Job)).Msg.Body {
			t.Errorf("job payload is wrong: %s", summary.Payload)
		}
	}
}
