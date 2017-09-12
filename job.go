package sqsd

import (
	"context"
	"time"
	"github.com/aws/aws-sdk-go/service/sqs"
	"net/http"
	"strings"
	"log"
	"bytes"
)

type SQSJob struct {
	Finished chan struct{}
	ID string
	Payload string
	StartAt time.Time
	URL string
	ContentType string
}

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

	resp, err := http.Post(j.URL, j.ContentType, strings.NewReader(j.Payload))
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if err != nil || statusCode != 200 {
		log.Printf("job[%s] failed. response: %s", j.ID, buf.String())
		cancel()
	}
	j.Finished <- struct {}{}
}