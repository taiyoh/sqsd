package sqsd

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestNewQueue(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	queue := NewQueue(sqsMsg)
	if queue == nil {
		t.Error("failed to create queue.")
	}
}

func TestQueueSummary(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId: aws.String("foobar"),
		Body:      aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
	}
	queue := NewQueue(sqsMsg)
	summary := queue.Summary()
	if summary.ID != queue.ID() {
		t.Error("different id")
	}
	if summary.ReceivedAt != queue.ReceivedAt.Unix() {
		t.Error("different received_at")
	}
	if summary.Payload != *queue.Msg.Body {
		t.Error("different payload")
	}
}
