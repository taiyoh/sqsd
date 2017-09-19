package sqsd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"testing"
)

func TestResource(t *testing.T) {
	c := &SQSMockClient{
		Resp: &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{
				&sqs.Message{
					MessageId:     aws.String("foo"),
					Body:          aws.String(`{"foo":"bar"}`),
					ReceiptHandle: aws.String("aaaaaaaaaaaaa"),
				},
			},
		},
	}
	r := &Resource{c, "http://example.com/foo"}
	if r == nil {
		t.Error("Resource object not created")
	}

	if _, err := r.GetMessages(); err != nil {
		t.Error("what's wrong???")
	}

	if err := r.DeleteMessage(c.Resp.Messages[0]); err != nil {
		t.Error("error founds")
	}
}
