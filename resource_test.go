package sqsd

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestResource(t *testing.T) {
	c := NewMockClient()
	c.Resp.Messages = []*sqs.Message{
		&sqs.Message{
			MessageId:     aws.String("foo"),
			Body:          aws.String(`{"foo":"bar"}`),
			ReceiptHandle: aws.String("aaaaaaaaaaaaa"),
		},
	}
	sc := SQSConf{URL: "http://example.com/foo", WaitTimeSec: 10}
	r := NewResource(c, sc)
	if r == nil {
		t.Error("Resource object not created")
	}

	ctx := context.Background()

	if _, err := r.GetMessages(ctx); err != nil {
		t.Error("what's wrong???")
	}

	if err := r.DeleteMessage(*c.Resp.Messages[0].ReceiptHandle); err != nil {
		t.Error("error founds")
	}
}
