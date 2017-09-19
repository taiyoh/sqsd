package sqsd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type Resource struct {
	Client sqsiface.SQSAPI
	URL    string
}

func (r *Resource) GetMessages() ([]*sqs.Message, error) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(r.URL),
		WaitTimeSeconds: aws.Int64(5),
	}
	resp, err := r.Client.ReceiveMessage(params)
	return resp.Messages, err
}

func (r *Resource) DeleteMessage(msg *sqs.Message) error {
	_, err := r.Client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.URL),
		ReceiptHandle: aws.String(*msg.ReceiptHandle),
	})
	return err
}
