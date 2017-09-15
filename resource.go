package sqsd

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type SQSResource struct {
	Client sqsiface.SQSAPI
	URL    string
}

func NewResource(client sqsiface.SQSAPI, url string) *SQSResource {
	return &SQSResource{client, url}
}

func (r *SQSResource) GetMessages(waitTimeout int64) ([]*sqs.Message, error) {
	params := sqs.ReceiveMessageInput{
		QueueUrl: aws.String(r.URL),
	}
	if waitTimeout > 0 {
		params.WaitTimeSeconds = aws.Int64(waitTimeout)
	}
	resp, err := r.Client.ReceiveMessage(&params)
	return resp.Messages, err
}

func (r *SQSResource) DeleteMessage(msg *sqs.Message) {
	r.Client.DeleteMessageRequest(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.URL),
		ReceiptHandle: aws.String(*msg.ReceiptHandle),
	})
}
