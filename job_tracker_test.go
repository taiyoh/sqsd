package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
	"testing"
)

func TestJobTracker(t *testing.T) {
	tracker := NewJobTracker(5)
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	job := &SQSJob{
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
	tracker.Delete(job)
	if _, exists := tracker.CurrentWorkings[job.ID()]; exists {
		t.Error("job not deleted")
	}

	for i := 0; i < tracker.MaxProcessCount; i++ {
		j := &SQSJob{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tracker.Add(j)
	}

	untrackedJob := &SQSJob{
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
}

func TestJobWorking(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := NewJobTracker(5)

	go func() {
		for {
			select {
			case shouldStop := <- tr.Pause():
				tr.JobWorking = shouldStop == false
			}
		}
	}()

	if !tr.JobWorking {
		t.Error("JobWorking false")
	}

	tr.Pause() <- true
	if tr.JobWorking {
		t.Error("JobWorking not changed to true")
	}

	tr.Pause() <- false
	if tr.JobWorking {
		t.Error("JobWorking not changed to false")
	}
}