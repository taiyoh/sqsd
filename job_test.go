package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"testing"
)

func TestNewJob(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	conf := &SQSDHttpWorkerConf{
		RequestContentType: "application/json",
		URL:                "http://example.com/foo/bar",
	}
	job := NewJob(sqsMsg, conf)
	if job == nil {
		t.Error("job load failed.")
	}
}

func TestJobFunc(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	ts := MockJobServer()
	defer ts.Close()

	t.Run("job failed", func(t *testing.T) {
		conf := &SQSDHttpWorkerConf{
			RequestContentType: "application/json",
			URL:                ts.URL + "/error",
		}
		job := NewJob(sqsMsg, conf)
		ctx := context.Background()
		ok, err := job.Run(ctx)
		if err != nil {
			t.Error("request error found")
		}
		if ok {
			t.Error("job request failed but finish status")
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		conf := &SQSDHttpWorkerConf{
			RequestContentType: "application/json",
			URL:                ts.URL + "/long",
		}
		job := NewJob(sqsMsg, conf)
		ctx, cancel := context.WithCancel(context.Background())
		e := make(chan error)
		go func() {
			_, err := job.Run(ctx)
			e <- err
		}()
		cancel()
		if err := <-e; err == nil {
			t.Error("error not found")
		}
	})

	t.Run("success", func(t *testing.T) {
		conf := &SQSDHttpWorkerConf{
			RequestContentType: "application/json",
			URL:                ts.URL + "/ok",
		}
		job := NewJob(sqsMsg, conf)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ok, err := job.Run(ctx)
		if err != nil {
			t.Error("request error founds")
		}
		if !ok {
			t.Error("job request finished but fail status")
		}
	})
}
