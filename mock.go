package sqsd

import (
	"context"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type SQSMockJob struct {
	SQSJobIFace
	msg      *sqs.Message
	status   bool
	err      error
	doneChan chan struct{}
}

func (m *SQSMockJob) ID() string {
	return *m.msg.MessageId
}

func (m *SQSMockJob) Msg() *sqs.Message {
	return m.msg
}

func (m *SQSMockJob) Run(ctx context.Context) (bool, error) {
	return m.status, m.err
}

func (m *SQSMockJob) Done() chan struct{} {
	return m.doneChan
}

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
