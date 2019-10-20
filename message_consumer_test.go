package sqsd_test

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/taiyoh/sqsd"
)

type HandleJobResponse struct {
	JobID string
	Err   error
}

func TestHandleJob(t *testing.T) {
	mc := sqsd.NewMockClient()
	sc := sqsd.SQSConf{URL: "http://example.com/foo/bar/queue", WaitTimeSec: 20}
	r := sqsd.NewResource(mc, sc)
	tr := sqsd.NewQueueTracker(5, sqsd.NewLogger("DEBUG"))

	receivedChan := make(chan *HandleJobResponse)
	msgc := sqsd.NewMessageConsumer(r, tr, "", sqsd.OnHandleJobEndFn(func(jobID string, err error) {
		receivedChan <- &HandleJobResponse{
			JobID: jobID,
			Err:   err,
		}
	}))

	ts := sqsd.MockServer()
	defer ts.Close()

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go msgc.Run(ctx, wg)

	t.Run("job failed", func(t *testing.T) {
		msgc.URL = ts.URL + "/error"
		queue := sqsd.NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageNG"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})

		tr.Register(queue)

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID {
			t.Error("wrong job processed")
		}

		if receivedRes.Err == nil {
			t.Error("error not found")
		}
	})

	t.Run("success", func(t *testing.T) {
		queue := sqsd.NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageOK"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		msgc.URL = ts.URL + "/ok"

		tr.Register(queue)

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID {
			t.Error("wrong job processed")
		}

		if receivedRes.Err != nil {
			t.Error("error found")
		}

	})

	t.Run("context cancelled", func(t *testing.T) {
		queue := sqsd.NewQueue(&sqs.Message{
			MessageId:     aws.String("TestHandleMessageErr"),
			Body:          aws.String(`{"hoge":"fuga"}`),
			ReceiptHandle: aws.String("aaaaaaaaaa"),
		})
		msgc.URL = ts.URL + "/long"

		tr.Register(queue)

		cancel()

		receivedRes := <-receivedChan

		if receivedRes.JobID != queue.ID {
			t.Error("wrong queue processed")
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
