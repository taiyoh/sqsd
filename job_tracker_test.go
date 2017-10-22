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

	job := &Job{
		Msg: &sqs.Message{
			MessageId: aws.String("foobar"),
			Body:      aws.String("hoge"),
		},
	}
	tracker.Add(job)
	if _, exists := tracker.CurrentWorkings[job.ID()]; !exists {
		t.Error("job not registered")
	}
	tracker.Delete(job)
	if _, exists := tracker.CurrentWorkings[job.ID()]; exists {
		t.Error("job not deleted")
	}

	jobKeys := []string{}
	for i := 0; i < tracker.MaxProcessCount; i++ {
		j := &Job{
			Msg: &sqs.Message{
				MessageId: aws.String("id:" + strconv.Itoa(i)),
				Body:      aws.String(`foobar`),
			},
		}
		tracker.Add(j)
		jobKeys = append(jobKeys, j.ID())
	}

	if len(tracker.Waitings) > 0 {
		t.Error("waitings exists")
	}

	untrackedJob := &Job{
		Msg: &sqs.Message{
			MessageId: aws.String("id:6"),
			Body:      aws.String("foobar"),
		},
	}
	blockerBroken := false
	go func() {
		untrackedJob.WaitUntilBlockerBroken()
		blockerBroken = true
	}()
	tracker.Add(untrackedJob)
	
	if len(tracker.Waitings) != 1 {
		t.Error("waitings not registered")
	}

	if tracker.Waitings[0].ID() != untrackedJob.ID() {
		t.Error("wrong job registered")
	}

	if tracker.Waitings[0].Blocker == nil {
		t.Error("blocker not exists")
	}

	if _, exists := tracker.CurrentWorkings[untrackedJob.ID()]; exists {
		t.Error("job registered ...")
	}
	if tracker.Acceptable() {
		t.Error("CurrentWorkings is filled but Acceptable() is invalid")
	}

	if blockerBroken {
		t.Error("blocker broken...")
	}

	deleteJob := tracker.CurrentWorkings[jobKeys[0]]
	tracker.Delete(deleteJob)

	if len(tracker.Waitings) != 0 {
		t.Error("waitings not cleared")
	}
	if _, exists := tracker.CurrentWorkings[untrackedJob.ID()]; !exists {
		t.Error("untracked job not moved")
	}
	if !blockerBroken {
		t.Error("blocker still exists")
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
