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

func (j *SQSJob) Run() bool {
	resp, err := http.Post(j.URL, j.ContentType, strings.NewReader(*j.Msg.Body))
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	statusCode := resp.StatusCode
	defer resp.Body.Close()
	if err != nil || statusCode != 200 {
		log.Printf("job[%s] failed. status: %d, response: %s", j.ID(), statusCode, buf.String())
		return false
	} else {
		return true
	}
}
