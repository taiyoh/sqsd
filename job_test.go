package sqsd

import (
	"context"
	"net/http"
	"fmt"
	"testing"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/aws"
	"net/http/httptest"
)

func TestNewJob(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body: aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	conf := &SQSDHttpWorkerConf{
		RequestContentType: "application/json",
		URL: "http://example.com/foo/bar",
	}
	job := NewJob(sqsMsg, conf)
	if job == nil {
		t.Error("job load failed.")
	}
}

func TestJobCancelled(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body: aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	ts := httptest.NewServer(http.HandlerFunc(
        func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "no gooooood")
			w.WriteHeader(http.StatusInternalServerError)
			w.Header().Set("content-Type", "text")
            return
        },
    ))
    defer ts.Close()

	conf := &SQSDHttpWorkerConf{
		RequestContentType: "application/json",
		URL: ts.URL,
	}

	job := NewJob(sqsMsg, conf)

	ctx := context.Background()

	go job.Run(ctx)

	select {
	case <- ctx.Done():
	}
}