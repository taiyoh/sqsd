package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"sync"
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
	ok := tracker.Add(job)
	if !ok {
		t.Error("job not inserted")
	}
	if _, exists := tracker.CurrentWorkings[job.ID()]; !exists {
		t.Error("job not registered")
	}
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	var deletedJobEventReceived bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-tracker.JobDeleted():
				mu.Lock()
				deletedJobEventReceived = true
				mu.Unlock()
				return
			}
		}
	}()
	if deletedJobEventReceived {
		t.Error("job deleted event comes")
	}
	time.Sleep(5 * time.Millisecond)
	tracker.Delete(job)
	wg.Wait()
	if _, exists := tracker.CurrentWorkings[job.ID()]; exists {
		t.Error("job not deleted")
	}
	if !deletedJobEventReceived {
		t.Error("job deleted event not comes")
	}

	for i := 0; i < tracker.MaxProcessCount; i++ {
		j := &Job{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tracker.Add(j)
	}

	mu.Lock()
	if len(tracker.Waitings) != 0 {
		t.Error("waiting jobs exists")
	}
	mu.Unlock()

	untrackedJob := &Job{
		Msg: &sqs.Message{
			MessageId: aws.String("id:6"),
			Body:      aws.String("foobar"),
		},
	}
	if ok := tracker.Add(untrackedJob); ok {
		t.Error("job register success...")
	}
	if _, exists := tracker.CurrentWorkings[untrackedJob.ID()]; exists {
		t.Error("job registered ...")
	}

	mu.Lock()
	if len(tracker.Waitings) != 1 {
		t.Error("waiting jobs not exists")
	}
	mu.Unlock()

	if tracker.Acceptable() {
		t.Error("CurrentWorkings is filled but Acceptable() is invalid")
	}
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
