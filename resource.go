package sqsd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSAPI provides partial interface for AWS SQS.
type SQSAPI interface {
	// sqsiface.SQSAPI
	ReceiveMessageWithContext(aws.Context, *sqs.ReceiveMessageInput, ...request.Option) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageWithContext(aws.Context, *sqs.DeleteMessageInput, ...request.Option) (*sqs.DeleteMessageOutput, error)
}

// Resource is wrapper for aws sqs library
type Resource struct {
	client      SQSAPI
	url         string
	waitTimeSec int64
}

// NewResource returns Resouce object
func NewResource(client SQSAPI, c SQSConf) *Resource {
	return &Resource{
		client:      client,
		url:         c.QueueURL(),
		waitTimeSec: c.WaitTimeSec,
	}
}

// GetMessages returns sqs.Message list using aws sqs library
func (r *Resource) GetMessages(ctx context.Context) ([]*sqs.Message, error) {
	resp, err := r.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(r.url),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(r.waitTimeSec),
	})

	return resp.Messages, err
}

// DeleteMessage provides queue deletion from SQS using aws sqs library
func (r *Resource) DeleteMessage(ctx context.Context, receipt string) error {
	_, err := r.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.url),
		ReceiptHandle: aws.String(receipt),
	})
	return err
}
