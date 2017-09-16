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
	Msg         *sqs.Message
	StartAt     time.Time
	URL         string
	ContentType string
}

func NewJob(msg *sqs.Message, conf *SQSDHttpWorkerConf) *SQSJob {
	return &SQSJob{
		Msg:         msg,
		StartAt:     time.Now(),
		URL:         conf.URL,
		ContentType: conf.RequestContentType,
	}
}

func (j *SQSJob) ID() string {
	return *j.Msg.MessageId
}

func (j *SQSJob) Run(ctx context.Context) (bool, error) {
	req, err := http.NewRequest("POST", j.URL, strings.NewReader(*j.Msg.Body))
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
	if err != nil {
		return false, err
	} else if statusCode != 200 {
		log.Printf("job[%s] failed. status: %d, response: %s", j.ID(), statusCode, buf.String())
		return false, nil
	}

	return true, nil
}
