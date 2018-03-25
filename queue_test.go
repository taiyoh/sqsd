package sqsd

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestQueueSummary(t *testing.T) {
	sqsMsg := &sqs.Message{
		MessageId:     aws.String("foobar"),
		Body:          aws.String(`{"from":"user_1","to":"room_1","msg":"Hello!"}`),
		ReceiptHandle: aws.String("reciept-foobar"),
	}
	queue := NewQueue(sqsMsg)
	summary := queue.Summary()
	if summary.ID != queue.ID {
		t.Error("different id")
	}
	if summary.ReceivedAt != queue.ReceivedAt.Unix() {
		t.Error("different received_at")
	}
	if summary.Payload != queue.Payload {
		t.Error("different payload")
	}
}
