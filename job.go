package sqsd

import (
	"context"
	"time"
	"github.com/aws/aws-sdk-go/service/sqs"
	"net/url"
	"net/http"
	"strings"
)

type SQSJob struct {
	Finished chan struct{}
	ID string
	Payload string
	StartAt time.Time
	URL string
	ContentType string
}

type SQSJobKey string

func NewJob(msg *sqs.Message, conf *SQSDHttpWorkerConf) *SQSJob {
	return &SQSJob{
		ID: *msg.MessageId,
		Payload: *msg.Body,
		StartAt : time.Now(),
		Finished : make(chan struct{}),
		URL : conf.URL,
		ContentType : conf.RequestContentType,
	}
}

func (j *SQSJob) Run(parent context.Context) {
	_, cancel := context.WithCancel(parent)
	defer cancel()

	uri, err := url.Parse(j.URL)
	if err != nil {
		cancel()
	}

	var resp *http.Response
	resp, err = http.Post(uri.String(), j.ContentType, strings.NewReader(j.Payload))
	if err != nil || resp.Status != "200" {
		cancel()
	}
	j.Finished <- struct {}{}
}