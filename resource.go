package sqsd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Resource is wrapper for aws sqs library
type Resource struct {
	Client      sqsiface.SQSAPI
	URL         string
	WaitTimeSec int64
}

// NewResource returns Resouce object
func NewResource(client sqsiface.SQSAPI, c SQSConf) *Resource {
	return &Resource{
		Client:      client,
		URL:         c.QueueURL(),
		WaitTimeSec: c.WaitTimeSec,
	}
}

// GetMessages returns sqs.Message list using aws sqs library
func (r *Resource) GetMessages(ctx context.Context) ([]*sqs.Message, error) {
	resp, err := r.Client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(r.URL),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(r.WaitTimeSec),
	})

	return resp.Messages, err
}

// DeleteMessage provides queue deletion from SQS using aws sqs library
func (r *Resource) DeleteMessage(receipt string) error {
	_, err := r.Client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.URL),
		ReceiptHandle: aws.String(receipt),
	})
	return err
}
