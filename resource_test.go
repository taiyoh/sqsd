package sqsd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"testing"
)

type SQSMockClient struct {
	sqsiface.SQSAPI
	Resp *sqs.ReceiveMessageOutput
}

func (c *SQSMockClient) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return c.Resp, nil
}

func (c *SQSMockClient) DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

func TestSQSResource(t *testing.T) {
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
	r := NewResource(c, "http://example.com/foo")
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
