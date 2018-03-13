package sqsd

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type HandleJobResponse struct {
	JobID string
	Ok    bool
	Err   error
}

func TestHandleJob(t *testing.T) {
	mc := NewMockClient()
	r := NewResource(mc, "http://example.com/foo/bar/queue")
	l := NewLogger("DEBUG")
	tr := NewJobTracker(5)
	msgc := NewMessageConsumer(r, tr, l, "")

	receivedChan := make(chan *HandleJobResponse)
	msgc.OnHandleJobEnds = func(jobID string, ok bool, err error) {
		receivedChan <- &HandleJobResponse{
			JobID: jobID,
			Ok:    ok,
			Err:   err,
		}
	}

	ctx := context.Background()

	ts := MockJobServer()
	defer ts.Close()

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go msgc.Run(ctx, wg)

	t.Run("job failed", func(t *testing.T) {
		msgc.URL = ts.URL + "/error"
		queue := NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})

		tr.Register(queue)

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID() {
			t.Error("wrong job processed")
		}

		if receivedRes.Ok {
			t.Error("error not returns")
		}

		if receivedRes.Err != nil {
			t.Error("error found")
		}
	})

	t.Run("success", func(t *testing.T) {
		queue := NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		msgc.URL = ts.URL + "/ok"

		tr.Register(queue)

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID() {
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
		queue := NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		msgc.URL = ts.URL + "/long"

		tr.Register(queue)

		cancel()

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID() {
			t.Error("wrong queue processed")
		}

		if receivedRes.Ok {
			t.Error("error not returns")
		}

		if receivedRes.Err == nil {
			t.Error("error not found")
		}
	})

	summaries := tr.CurrentSummaries()
	if len(summaries) != 0 {
		t.Error("job remains")
	}

	wg.Wait()
}
