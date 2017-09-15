package sqsd

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"net/http"
	"net/http/httptest"
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

func TestJobFailed(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("content-Type", "text")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "no goood")
			return
		},
	))
	defer ts.Close()

	conf := &SQSDHttpWorkerConf{
		RequestContentType: "application/json",
		URL:                ts.URL,
	}

	job := NewJob(sqsMsg, conf)

	ok := job.Run()

	if ok {
		t.Error("job request failed but finish status")
	}
}

func TestJobSucceed(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}

	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("content-Type", "text")
			fmt.Fprintf(w, "goood")
			return
		},
	))
	defer ts.Close()

	conf := &SQSDHttpWorkerConf{
		RequestContentType: "application/json",
		URL:                ts.URL,
	}

	job := NewJob(sqsMsg, conf)

	ok := job.Run()

	if !ok {
		t.Error("job request finished but fail status")
	}
}
