package sqsd

import (
	"testing"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"strconv"
)

func TestJobTracker(t *testing.T) {
	tracker := NewJobTracker(5)
	if tracker == nil {
		t.Error("job tracker not loaded.")
	}

	job := &SQSJob{
		Msg: &sqs.Message{
			MessageId: aws.String("foobar"),
			Body: aws.String("hoge"),
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
				Body: aws.String(`foobar`),
			},
		}
		tracker.Add(j)
	}

	untrackedJob := &SQSJob{
		Msg: &sqs.Message{
			MessageId: aws.String("id:6"),
			Body: aws.String("foobar"),
		},
	}
	if ok := tracker.Add(untrackedJob); ok {
		t.Error("job register success...")
	}
	if _, exists := tracker.CurrentWorkings[untrackedJob.ID()]; exists {
		t.Error("job registered ...")
	}
}