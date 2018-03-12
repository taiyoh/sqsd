package sqsd

import (
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Job struct {
	Msg        *sqs.Message
	ReceivedAt time.Time
}

type JobSummary struct {
	ID         string `json:"id"`
	ReceivedAt int64  `json:"received_at"`
	Payload    string `json:"payload"`
}

func NewJob(msg *sqs.Message) *Job {
	return &Job{
		Msg:        msg,
		ReceivedAt: time.Now(),
	}
}

func (j *Job) ID() string {
	return *j.Msg.MessageId
}

func (j *Job) Summary() *JobSummary {
	return &JobSummary{
		ID:         j.ID(),
		ReceivedAt: j.ReceivedAt.Unix(),
		Payload:    *j.Msg.Body,
	}
}
