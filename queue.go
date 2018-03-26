package sqsd

import (
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type Queue struct {
	ID         string
	Payload    string
	Receipt    string
	ReceivedAt time.Time
}

type QueueSummary struct {
	ID         string `json:"id"`
	ReceivedAt int64  `json:"received_at"`
	Payload    string `json:"payload"`
}

func NewQueue(msg *sqs.Message) Queue {
	return Queue{
		ID:         *msg.MessageId,
		Payload:    *msg.Body,
		Receipt:    *msg.ReceiptHandle,
		ReceivedAt: time.Now(),
	}
}

func (q Queue) Summary() QueueSummary {
	return QueueSummary{
		ID:         q.ID,
		ReceivedAt: q.ReceivedAt.Unix(),
		Payload:    q.Payload,
	}
}
