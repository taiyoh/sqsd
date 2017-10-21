package sqsd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type Resource struct {
	Client        sqsiface.SQSAPI
	ReceiveParams *sqs.ReceiveMessageInput
}

func NewResource(client sqsiface.SQSAPI, url string) *Resource {
	return &Resource{
		Client: client,
		ReceiveParams: &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(url),
			MaxNumberOfMessages: aws.Int64(10),
			WaitTimeSeconds:     aws.Int64(20),
		},
	}
}

func (r *Resource) GetMessages(ctx context.Context) ([]*sqs.Message, error) {
	resp, err := r.Client.ReceiveMessageWithContext(ctx, r.ReceiveParams)
	return resp.Messages, err
}

func (r *Resource) DeleteMessage(msg *sqs.Message) error {
	_, err := r.Client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      r.ReceiveParams.QueueUrl,
		ReceiptHandle: aws.String(*msg.ReceiptHandle),
	})
	return err
}
