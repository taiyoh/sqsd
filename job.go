package sqsd

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"net/http"
	"strings"
	"time"
)

type SQSJob struct {
	msg         *sqs.Message
	StartAt     time.Time
	URL         string
	ContentType string
	doneChan    chan struct{}
}

func NewJob(msg *sqs.Message, conf *SQSDHttpWorkerConf) *SQSJob {
	return &SQSJob{
		msg:         msg,
		StartAt:     time.Now(),
		URL:         conf.URL,
		ContentType: conf.RequestContentType,
		doneChan:    make(chan struct{}),
	}
}

func (j *SQSJob) ID() string {
	return *j.Msg().MessageId
}

func (j *SQSJob) Msg() *sqs.Message {
	return j.msg
}

func (j *SQSJob) Run(ctx context.Context) (bool, error) {
	req, err := http.NewRequest("POST", j.URL, strings.NewReader(*j.msg.Body))
	if err != nil {
		return false, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", j.ContentType)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if statusCode != 200 {
		log.Printf("job[%s] failed. status: %d, response: %s", j.ID(), statusCode, buf.String())
		return false, nil
	}

	return true, nil
}

func (j *SQSJob) Done() chan struct{} {
	return j.doneChan
}
