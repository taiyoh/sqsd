package sqsd

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestNewJob(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	job := NewJob(sqsMsg)
	if job == nil {
		t.Error("job load failed.")
	}
}

func TestJobSummary(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	job := NewJob(sqsMsg)
	summary := job.Summary()
	if summary.ID != job.ID() {
		t.Error("different id")
	}
	if summary.ReceivedAt != job.ReceivedAt.Unix() {
		t.Error("different received_at")
	}
	if summary.Payload != *job.Msg.Body {
		t.Error("different payload")
	}
}
