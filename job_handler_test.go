package sqsd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestHandleJob(t *testing.T) {
	c := &WorkerConf{}
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	tr := NewJobTracker(5)
	h := NewJobHandler(r, tr)

	receivedChan := make(chan *HandleJobResponse)
	h.OnHandleJobEnds = func(res *HandleJobResponse) {
		receivedChan <- res
	}

	ctx := context.Background()

	ts := MockJobServer()
	defer ts.Close()

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())

	h.RunEventListener(ctx, wg)

	t.Run("job failed", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, c)
		job.URL = ts.URL + "/error"

		tr.Register(job)

		receivedRes := <- receivedChan

		if receivedRes.JobID != job.ID() {
			t.Error("wrong job processed")
		}

		if receivedRes.Ok {
			t.Error("error not returns")
		}

		if receivedRes.Err == nil {
			t.Error("error not found")
		}
	})

	t.Run("success", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, c)
		job.URL = ts.URL + "/ok"

		tr.Register(job)

		receivedRes := <- receivedChan

		if receivedRes.JobID != job.ID() {
			t.Error("wrong job processed")
		}

		if !receivedRes.Ok {
			t.Error("error returns")
		}

		if receivedRes.Err != nil {
			t.Error("error found")
		}

	})

	t.Run("context cancelled", func(t *testing.T) {
		job := NewJob(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		}, c)
		job.URL = ts.URL + "/long"
	
		tr.Register(job)

		cancel()

		receivedRes := <- receivedChan

		if receivedRes.JobID != job.ID() {
			t.Error("wrong job processed")
		}

		if receivedRes.Ok {
			t.Error("error not returns")
		}

		if receivedRes.Err != nil {
			t.Error("error found")
		}
	})

	summaries := tr.CurrentSummaries()
	if len(summaries) != 0 {
		t.Error("job remains")
	}

	wg.Wait()
}
