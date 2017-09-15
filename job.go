package sqsd

import (
	"bytes"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"net/http"
	"strings"
	"time"
)

type SQSJob struct {
	Finished    chan struct{}
	Failed      chan struct{}
	ID          string
	Payload     string
	StartAt     time.Time
	URL         string
	ContentType string
}

func NewJob(msg *sqs.Message, conf *SQSDHttpWorkerConf) *SQSJob {
	return &SQSJob{
		ID:          *msg.MessageId,
		Payload:     *msg.Body,
		StartAt:     time.Now(),
		Finished:    make(chan struct{}),
		Failed:      make(chan struct{}),
		URL:         conf.URL,
		ContentType: conf.RequestContentType,
	}
}

func (j *SQSJob) Run() {
	resp, err := http.Post(j.URL, j.ContentType, strings.NewReader(j.Payload))
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if err != nil || statusCode != 200 {
		log.Printf("job[%s] failed. status: %d, response: %s", j.ID, statusCode, buf.String())
		j.Failed <- struct{}{}
	} else {
		j.Finished <- struct{}{}
	}
}
